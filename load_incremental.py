import os
import io
import boto3
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()

def create_pg_connection():
    database=os.environ.get('POSTGRE_DATABASE') 
    user=os.environ.get('POSTGRE_USER')
    password=os.environ.get('POSTGRE_PASSWORD')
    host=os.environ.get('POSTGRE_HOST')
    port=os.environ.get('POSTGRE_PORT')
    
    try:
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}', poolclass=NullPool)
        conn = engine.connect()
    except:
        conn.close()
        raise Exception("Failed to create database connection")
    
    return conn


def read_checkpoint(table_name):
    try:
        conn = create_pg_connection()
        q_result = conn.execute(text(f"SELECT checkpoint FROM checkpoints WHERE table_name = '{table_name}'"))
        
        for q in q_result:
            data = q[0]
    except:
        raise Exception("Failed to read checkpoint")
    finally:
        conn.close()
    
    return data


def insert_on_conflict_update(table, conn, keys, data_iter):
    #Using text clause is impractical here
    #Falling back to ORM

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    
    conn.execute(upsert_statement)
    

# list minio bucket
def list_s3_objects(bucket_name):
    blobs_name = []
    
    try:
        s3_client = create_s3_client()
            
        response = s3_client.list_objects(
            Bucket=bucket_name
        )
    except:
        raise Exception("Failed to list objects")
    
    contents = response.get('Contents')
    
    if contents is not None:
        for blob in contents:
            blobs_name.append(blob.get('Key'))
            
    return blobs_name


def get_s3_object(bucket_name, object_key):
    try:
        s3_client = create_s3_client()
        
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
    except:
        raise Exception("Failed to get object")
    
    return response


# create minio connection
def create_s3_client():
    session = boto3.session.Session()
    s3_client = session.client(
                    service_name='s3',
                    aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('MINIO_SECRET_ACCESS_KEY'),
                    endpoint_url=os.environ.get('MINIO_URL'),
                )
    
    return s3_client


if __name__ == "__main__":
    # loop minio bucket
    bucket_name = os.environ.get('MINIO_BUCKET_NAME')
    filenames = list_s3_objects(bucket_name)
    db_conn= create_pg_connection()
    primary_key_map = {'employees': 'employe_id', 'timesheets': 'timesheet_id'}

    for file in filenames:
        new_checkpoint = ''
        # extract file name to be table name
        table_name = file.split('/')[1].split('.')[0]

        # read file
        s3_obj = get_s3_object(bucket_name, file)
        decoded_s3_obj = s3_obj.get('Body').read().decode('utf-8')
        df = pd.read_csv(io.StringIO(decoded_s3_obj))
        
        # read checkpoints table (table, checkpoint (date))
        checkpoint = read_checkpoint(table_name)
        
        df = df.drop_duplicates(subset=primary_key_map[table_name], keep="last")
        if table_name == 'employees':
            if checkpoint is None:
                # load all data
                df.to_sql(f'{table_name}',if_exists='append',
                    index=False, con=db_conn,method=insert_on_conflict_update)
            else:
                # load only new data relative to the checkpoint (latest date in the dataset)
                filtered_df = df.loc[(pd.to_datetime(df['join_date']).dt.date > checkpoint) 
                                    | (pd.to_datetime(df['resign_date']).dt.date > checkpoint)]
                print(filtered_df)
                
                filtered_df.to_sql(f'{table_name}', 
                    con=db_conn, index=False, if_exists='append', 
                    method=insert_on_conflict_update)

            dt_resign = pd.to_datetime(df['resign_date']).max()
            dt_join = pd.to_datetime(df['join_date']).max()
            new_checkpoint = max(dt_resign, dt_join).date()        
        elif table_name == 'timesheets':
            df["checkin"] = pd.to_datetime(df['checkin'],format='%H:%M:%S')
            df["checkout"] = pd.to_datetime(df['checkout'],format='%H:%M:%S')

            if checkpoint is None:
                #load all data
                df.to_sql(f'{table_name}',if_exists='append',
                    index=False, con=db_conn,method=insert_on_conflict_update)
            else:
                # load only new data relative to the checkpoint (latest date in the dataset)
                filtered_df = df.loc[pd.to_datetime(df['date']).dt.date > checkpoint]
                print(filtered_df)
                filtered_df.to_sql(f'{table_name}', 
                    con=db_conn, index=False, if_exists='append', 
                    method=insert_on_conflict_update)
                
            new_checkpoint = df['date'].max()

        print(f"Table {table_name} data are loaded")
        
        # update checkpoint
        db_conn.execute(text(f"UPDATE checkpoints SET checkpoint = '{new_checkpoint}' WHERE table_name = '{table_name}'"))
        db_conn.commit()
    
    db_conn.close()
