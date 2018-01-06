--Find top 5 job titles who are having highest avg growth in applications.
data  =  LOAD  '/niit/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data;
data = foreach data generate $4 as job, $7 as year;
--dump data;
dat = filter data by year=='2011';
a = group dat by job;
--dump a;
b1 = foreach a generate group as job, (float)COUNT(dat.job) as c1;
--dump b1;
dat = filter data by year=='2012';
a = group dat by job;
--dump a;
b2 = foreach a generate group as job, (float)COUNT(dat.job) as c2;
--dump b2;
dat = filter data by year=='2013';
a = group dat by job;
--dump a;
b3 = foreach a generate group as job, (float)COUNT(dat.job) as c3;
--dump b3;
dat = filter data by year=='2014';
a = group dat by job;
--dump a;
b4 = foreach a generate group as job, (float)COUNT(dat.job) as c4;
--dump b4;
dat = filter data by year=='2015';
a = group dat by job;
--dump a;
b5 = foreach a generate group as job, (float)COUNT(dat.job) as c5;
--dump b5;
dat = filter data by year=='2016';
a = group dat by job;
--dump a;
b6 = foreach a generate group as job, (float)COUNT(dat.job) as c6;
--dump b6;


c = join b1 by $0, b2 by $0;
c = foreach c generate $0, $1 as a11, $3 as a12;
--dump c;
d1 = foreach c generate $0, (a12-a11)/a11 *100 as growth;
--dump d1;
c = join b2 by $0, b3 by $0;
c = foreach c generate $0, $1 as a12, $3 as a13;
--dump c;
d2 = foreach c generate $0, (a13-a12)/a12 *100 as growth;
--dump d2;
c = join b3 by $0, b4 by $0;
c = foreach c generate $0, $1 as a13, $3 as a14;
--dump c;
d3 = foreach c generate $0, (a14-a13)/a13 *100 as growth;
--dump d3;
c = join b4 by $0, b5 by $0;
c = foreach c generate $0, $1 as a14, $3 as a15;
--dump c;
d4 = foreach c generate $0, (a15-a14)/a14 *100 as growth;
--dump d4;
c = join b5 by $0, b6 by $0;
c = foreach c generate $0, $1 as a15, $3 as a16;
--dump c;
d5 = foreach c generate $0, (a16-a15)/a15 *100 as growth;
--dump d1;


final = join d1 by $0, d2 by $0, d3 by $0, d4 by $0, d5 by $0;
final = foreach final generate $0,$1,$3,$5,$7,$9;
--dump final;

final1 = foreach final generate $0, ($1+$2+$3+$4+$5)/5 as avg;
--dump final1;

e = limit( order final1 by avg desc) 5;
dump e;
