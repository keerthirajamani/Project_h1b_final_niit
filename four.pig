

A = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitude:double,latitude:double);


--select employer_name,count(case_status)as total,year from h1b_final where year='2016'  group by employer_name,year order by total desc limit 5;

			--2011

B1 = filter A by year =='2011' ;

C1 = group B1 by employer_name;

D1 = foreach C1 generate group,COUNT(B1.case_status) as total1,'2011';

E1 = order D1 by total1 desc ;

F1 = limit E1 5;

--dump F1;
			--2012

B2 = filter A by year =='2012' ;

C2 = group B2 by employer_name;

D2 = foreach C2 generate group,COUNT(B2.case_status) as total2,'2012';

E2 = order D2 by total2 desc ;

F2 = limit E2 5;

--dump F2;
			--2013
B3 = filter A by year =='2013' ;

C3 = group B3 by employer_name;

D3 = foreach C3 generate group,COUNT(B3.case_status) as total3,'2013';

E3 = order D3 by total3 desc ;

F3 = limit E3 5;

--dump F3;
			--2014
B4 = filter A by year =='2014' ;

C4 = group B4 by employer_name;

D4 = foreach C4 generate group,COUNT(B4.case_status) as total4,'2014';

E4 = order D4 by total4 desc ;

F4 = limit E4 5;

--dump F4;
			--2015
B5 = filter A by year =='2015' ;

C5 = group B5 by employer_name;

D5 = foreach C5 generate group,COUNT(B5.case_status) as total5,'2015';

E5 = order D5 by total5 desc ;

F5 = limit E5 5;

--dump F5;
			--2016
B6 = filter A by year =='2016' ;

C6 = group B6 by employer_name;

D6 = foreach C6 generate group,COUNT(B6.case_status) as total6,'2016';

E6 = order D6 by total6 desc ;

F6 = limit E6 5;

G = UNION F1,F2,F3,F4,F5,F6;

H = order G by $2;
dump H;

--store H INTO '/niit/pigop/four' USING PigStorage(',');


