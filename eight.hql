



			use project;
			--For full time position
select year,job_title,avg(prevailing_wage) as tot,full_time_position from h1b_final where (case_status='CERTIFIED' OR case_status='CERTIFIED WITHDRAWN') AND full_time_position="Y" group by job_title,year,full_time_position order by tot desc;



			-- For part time position
select year,job_title,avg(prevailing_wage) as tot,full_time_position from h1b_final where (case_status='CERTIFIED' OR case_status='CERTIFIED WITHDRAWN') AND full_time_position="N" group by job_title,year,full_time_position order by tot desc;

