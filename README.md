# salary-per-hour
Below are the steps to test the scripts:
1. Install python dependencies by running `pip3 install -r requirements.txt`
2. Execute `docker compose up`. Wait until all dependencies are running mainly the `minio-seeder`.
3. Run load_incremental.py script
4. Connect to the postgresql provisioned by docker compose. DB Name: myuser or whatever value you set POSTGRES_USER in Docker Compose file.
5. Execute query_full_snapshot.sql script. Please ensure to run step 3 before running this.
6. The calculation is in the table `presentation_salary_per_hour`

Assumptions:
- The tasks will be run sequentially in the scheduler, where the sql query to calculate salary per hour will be executed after the data are loaded from the source incrementally on a daily basis
- The source data are csv files located in S3/Minio bucket
- The incoming data are in the future date relative to the date in the files currently
- If there are any duplicates in the data source, the data considered is the latest data
- The start date of the calculation is the earliest month in the initial dataset
- The work hour calculation for each employee on each date will be rounded down to the next whole number
- If an employee did not checkin but did checkout or otherwise, then the work hour for the employee on that date will be considered as the normal working hours, which is 8 hours
- If an employee did not checkin nor checkout, then the work hour for the employee on that date is 0 hour 
- If an employee checkin time is later than the checkout time, then the work hour for the employee on that date will be calculated as checkin - checkout. It is clearly a bug that should be handled in the application for the next iteration.