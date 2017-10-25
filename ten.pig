A = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitude:double,latitude:double);

B = group A by job_title;
C = foreach B generate group,COUNT(A.case_status) as total;
B1 = filter A by case_status=='CERTIFIED';
C1 = group B1 by job_title;
D1 = foreach C1 generate group, COUNT(B1.case_status) as total1;
B2 = filter A by case_status =='CERTIFIED-WITHDRAWN';
C2 = group B2 by job_title;
D2 = foreach C2 generate group, COUNT(B2.case_status) as total2;
E = join C by $0,D1 by $0,D2 by $0;
--dump E;
F = foreach E generate $0,$1,$3,$5;
--dump F;
Succ = foreach F generate $0,(float)($3+$2)/($1)*100,$1;
--dump Succ;
final = filter Succ by $1>70 and $2>1000;
dump final;
store final INTO '/niit/pigop/ten' USING PigStorage(',');

