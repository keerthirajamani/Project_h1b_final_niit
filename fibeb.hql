use project;
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2011"  group by job_title,year order by total desc limit 10; 
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2012"  group by job_title,year order by total desc limit 10;
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2013"  group by job_title,year order by total desc limit 10;
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2014"  group by job_title,year order by total desc limit 10;
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2015"  group by job_title,year order by total desc limit 10;
select job_title,count(case_status) as total,year from h1b_final where case_status="CERTIFIED" and year="2016"  group by job_title,year order by total desc limit 10;
