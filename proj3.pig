data  =  LOAD  '/home/hduser/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data;
a = foreach data generate $1 as ca, $3 as soc, $4 as job;
--dump a;
b = filter a by ca=='CERTIFIED' and job=='DATA SCIENTIST';
--dump b;
c = group b by soc;
--dump c;
d = foreach c generate group as soc, COUNT(b.job) as count;
--dump d;
e = limit(order d by count desc) 1;
dump e;

--(STATISTICIANS,369)

