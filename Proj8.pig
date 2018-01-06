data  =  LOAD  '/home/hduser/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
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
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s1 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2011Y';


d = filter c by year=='2012';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s2 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2012Y';



d = filter c by year=='2013';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s3 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2013Y';


d = filter c by year=='2014';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s4 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2014Y';

d = filter c by year=='2015';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s5 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2015Y';

d = filter c by year=='2016';
--dump d;
a = group d by job;
--dump a;
b = foreach a generate group as job,AVG(d.wage) as avg;
--dump b;
s6 = order b by avg desc ;
--dump s;
--q = store s into '/home/hduser/output_Proj8_2016Y';

xx = union s1,s2,s3,s4,s5,s6;
dump xx;




