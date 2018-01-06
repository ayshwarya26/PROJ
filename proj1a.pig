data  =  LOAD  '/home/hduser/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data;
data = foreach data generate $4 as job, $7 as year;
--dump data;
b = filter data by job=='DATA ENGINEER';
--dump b;
a = group b by year;
--dump a;
c = foreach a generate group as year, COUNT(b.job);
dump c;

aa = store c into "home/hduser/O_qq";
--select year,COUNT(job_title) from h1b_final where job_title=="DATA ENGINEER" group by year;


--(2011,18)
--(2012,32)
--(2013,41)
--(2014,89)
--(2015,160)
--(2016,251)

