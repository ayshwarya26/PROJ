--Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]

data  =  LOAD  '/niit/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data;
data = foreach data generate $1 as status,$4 as job, $5 as fp, $6 as wage, $7 as year;
--dump data;
x = filter data by status=='CERTIFIED' or status=='CERTIFIED-WITHDRAWN';
--dump x;

c = filter x by fp== 'Y';
--dump c;
d = filter c by year=='2011';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s1 = distinct b;
--dump s1;

d = filter c by year=='2012';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s2 = distinct b;
--dump s2;

d = filter c by year=='2013';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s3 = distinct b;
--dump s3;

d = filter c by year=='2014';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s4 = distinct b;
--dump s4;

d = filter c by year=='2015';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s5 = distinct b;
--dump s5;

d = filter c by year=='2016';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg,flatten(d.year) as year;
--dump b;
s6 = distinct b;
--dump s6;

xx = union s1, s2,s3,s4,s5,s6;
xx = order xx by $2 asc ,$1  desc;
dump xx;






