load_data = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);

filter_data = filter load_data by year =='2011';

group_data = group filter_data by job_title;

count_data = foreach group_data generate group,COUNT($1) as total;

order_data = order count_data by total  desc ;

--dump order_data ;



filter_data2 = filter load_data by year =='2012';

group_data2 = group filter_data2 by job_title;

count_data2 = foreach group_data2 generate group,COUNT($1) as total2;

order_data2 = order count_data2 by total2  desc  ;

--dump order_data2 ;

filter_data3 = filter load_data by year =='2013';

group_data3 = group filter_data3 by job_title;

count_data3 = foreach group_data3 generate group,COUNT($1) as total3;

order_data3 = order count_data3 by total3 desc ;

--dump order_data3 ;

filter_data4 = filter load_data by year =='2014';

group_data4 = group filter_data4 by job_title;

count_data4 = foreach group_data4 generate group,COUNT($1) as total4;

order_data4 = order count_data4 by total4 desc ;

filter_data5 = filter load_data by year =='2015';

group_data5 = group filter_data5 by job_title;

count_data5 = foreach group_data5 generate group,COUNT($1) as total5;

order_data5 = order count_data5 by total5  desc ;

filter_data6 = filter load_data by year =='2016';

group_data6 = group filter_data6 by job_title;

count_data6 = foreach group_data6 generate group,COUNT($1) as total6;

order_data6 = order count_data6 by total6  desc ;

join_data = join order_data by $0,order_data2 by $0,order_data3 by $0,order_data4 by $0,order_data5 by $0,order_data6 by $0;

--dump join_data ;

generate_data = foreach join_data generate $0,$1,$3,$5,$7,$9,$11;

--dump generate_data ;
progressive_growth = foreach generate_data generate $0,(float)($6-$5)/$5*100,(float)($5-$4)/$4*100,(float)($4-$3)/$3*100,(float)($3-$2)/$2*100,(float)($2-$1)/$1*100;

average_growth = foreach progressive_growth generate $0,(float)($1+$2+$3+$4+$5)/5 as avg ;

order_average = limit(order average_growth by avg desc) 5;

dump order_average;



/*
(SENIOR SYSTEMS ANALYST JC60,4255.4)
(SOFTWARE DEVELOPER 2,3480.8)
(PROJECT MANAGER 3,3233.4)
(SYSTEMS ANALYST JC65,2985.0)
(MODULE LEAD,2917.2)
*/
