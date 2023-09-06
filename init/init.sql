CREATE TABLE IF NOT EXISTS employees(
    employe_id integer PRIMARY KEY,
    branch_id integer,
    salary integer,
    join_date date,
    resign_date date
);

CREATE TABLE IF NOT EXISTS timesheets(
    timesheet_id integer PRIMARY KEY,
    employee_id integer,
    date date,
    checkin timestamp,
    checkout timestamp
);

CREATE TABLE IF NOT EXISTS checkpoints(
    id serial PRIMARY KEY,
    table_name varchar(30),
    checkpoint date
);

create table if not exists presentation_salary_per_hour(
	year integer,
	month integer,
	branch_id integer,
	salary_per_hour float,
	total_salary bigint,
	total_hours integer,
	UNIQUE (year, month, branch_id, salary_per_hour, total_salary, total_hours)
);

INSERT INTO checkpoints (table_name) VALUES ('employees');
INSERT INTO checkpoints (table_name) VALUES ('timesheets');
-- \copy employees (employee_id, branch_id, salary, join_date, resign_date) FROM '/var/lib/postgresql/init-data/employees.csv' CSV HEADER;
-- \copy timesheets (timesheet_id, employee_id, date, checkin, checkout) FROM '/var/lib/postgresql/init-data/timesheets.csv' CSV HEADER;
