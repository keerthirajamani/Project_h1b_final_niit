A = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitude:double,latitude:double);


--select job_title,count(*)as total,year from h1b_final where year='2016' group by job_title,year order by total desc limit 10;

			--2011

B1 = filter A by year =='2011' ;

C1 = group B1 by job_title;

D1 = foreach C1 generate group,COUNT(B1.job_title) as total1,'2011';

E1 = order D1 by total1 desc ;

F1 = limit E1 10;

--dump F1;
			--2012

B2 = filter A by year =='2012' ;

C2 = group B2 by job_title;

D2 = foreach C2 generate group,COUNT(B2.job_title) as total2,'2012';

E2 = order D2 by total2 desc ;

F2 = limit E2 10;

--dump F2;
			--2013

B3 = filter A by year =='2013' ;

C3 = group B3 by job_title;

D3 = foreach C3 generate group,COUNT(B3.job_title) as total3,'2013';

E3 = order D3 by total3 desc ;

F3 = limit E3 10;

--dump F3;
			--2014

B4 = filter A by year =='2014' ;

C4 = group B4 by job_title;

D4 = foreach C4 generate group,COUNT(B4.job_title) as total4,'2014';

E4 = order D4 by total4 desc ;

F4 = limit E4 10;

--dump F4;
			--2015

B5 = filter A by year =='2015' ;

C5 = group B5 by job_title;

D5 = foreach C5 generate group,COUNT(B5.job_title) as total5,'2015';

E5 = order D5 by total5 desc ;

F5 = limit E5 10;

--dump F5;
			--2016

B6 = filter A by year =='2016' ;

C6 = group B6 by job_title;

D6 = foreach C6 generate group,COUNT(B6.job_title) as total6,'2016';

E6 = order D6 by total6 desc ;

F6 = limit E6 10;

--dump F6;
G = UNION F1,F2,F3,F4,F5,F6;

H = order G by $2;
dump H;
/*store H INTO '/niit/pigop/fivea' USING PigStorage(',');*/








