insert into presentation_salary_per_hour (
with emp_data as (
	select 
		employe_id,
		branch_id,
		salary,
		join_date,
		date_trunc('month', join_date) as join_month,
		resign_date,
		date_trunc('month', resign_date) as resign_month 
	from employees e 
),
daterange AS (
  SELECT generate_series(date '2016-08-01', now(), '1 month') as month_in_range 
),
emp_temp as (
	select 
		employe_id,
		branch_id,
		salary,
		join_date,
		join_month,
		resign_date,
		resign_month,
		LEAD(resign_month) OVER(PARTITION BY employe_id ORDER BY resign_month) AS stop_flag 
	from 
		emp_data
),
active_employees as (
WITH temp_active_employee AS (
  SELECT
    month_in_range,
    employe_id,
    branch_id,
    salary,
    join_month,
    resign_month,
    row_number() over(partition by month_in_range, employe_id order by month_in_range desc) as rn 
  FROM
    daterange
  JOIN
    emp_temp
  ON
    daterange.month_in_range >= emp_temp.join_month and daterange.month_in_range <= emp_temp.resign_month
    AND (daterange.month_in_range < emp_temp.stop_flag OR emp_temp.stop_flag IS NULL)
  ORDER BY
    employe_id, month_in_range
)
SELECT 
  month_in_range,
  employe_id,
  branch_id,
  salary,
  join_month,
  resign_month
FROM temp_active_employee 
WHERE rn = 1),
work_hours as (
	select 
		employee_id,
		date_trunc('month', date) as month,
		case when (checkin is null and checkout is not null) or (checkin is not null and checkout is null) then 8
		when (checkin is not null and checkout is not null) and (checkin > checkout) then sum(floor(extract(epoch from(checkin - checkout)) / 3600))
		when checkin is not null and checkout is not null then sum(floor(extract(epoch from(checkout - checkin)) / 3600)) 
		else 0 end as hours
	from timesheets t 
	group by employee_id, date, checkin, checkout
) 
select 
	date_part('year', month_in_range) as year,
	date_part('month', month_in_range) as month, 
	branch_id,
	case when hours = 0 then sum(salary) 
	else sum(salary) / sum(hours) end as salary_per_hour,
	sum(salary) as total_salary,
	sum(hours) as total_hours
from active_employees ae 
left join work_hours wh 
on ae.employe_id = wh.employee_id 
group by month_in_range, branch_id, hours)
on conflict (year, month, branch_id, salary_per_hour, total_salary, total_hours) do update 
set year = excluded.year, month = excluded.month, branch_id = excluded.branch_id, salary_per_hour = excluded.salary_per_hour, total_salary = excluded.total_salary, total_hours = excluded.total_hours;
