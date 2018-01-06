data  =  LOAD  '/home/hduser/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data;
data = foreach data generate $1 as s, $7 as year;
--dump data;
--q = group data all;
--r = foreach q generate (float)COUNT(data.s) as tot;
--dump r;
x = filter data by year=='2011';
--dump x;
q = group x all;
r = foreach q generate (float)COUNT(x.s) as tot;
a = group x by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x.s) as c, FLATTEN(x.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w1 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;
--ww = foreach w generate $3/$1;
--dump w1;


x1 = filter data by year=='2012';
--dump x;
q = group x1 all;
r = foreach q generate (float)COUNT(x1.s) as tot;
a = group x1 by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x1.s) as c, FLATTEN(x1.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w2 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;
--dump w2;

x = filter data by year=='2013';
--dump x;
q = group x all;
r = foreach q generate (float)COUNT(x.s) as tot;
a = group x by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x.s) as c, FLATTEN(x.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w3 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;
--dump w3;


x = filter data by year=='2014';
--dump x;
q = group x all;
r = foreach q generate (float)COUNT(x.s) as tot;
a = group x by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x.s) as c, FLATTEN(x.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w4 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;
--dump w4;

x = filter data by year=='2015';
--dump x;
q = group x all;
r = foreach q generate (float)COUNT(x.s) as tot;
a = group x by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x.s) as c, FLATTEN(x.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w5 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;

--dump w5;

x = filter data by year=='2016';
--dump x;
q = group x all;
r = foreach q generate (float)COUNT(x.s) as tot;
a = group x by $0;
--dump a;
b = foreach a generate group as status, (float)COUNT(x.s) as c, FLATTEN(x.year);
--dump b; 
a11 = DISTINCT b;
--dump a11;
w6 = foreach a11 generate $0, $1 as cc, $2 , ROUND_TO((c*100)/r.tot ,2) ;

--dump w6;
qq = union w1,w2,w3,w4,w5,w6;
--dump qq;
pp = order qq by $0 asc,$2 asc ;
--dump pp;
--ww= order pp by $0 asc;
--dump ww;
a =store pp into '/home/hduser/output_c';

