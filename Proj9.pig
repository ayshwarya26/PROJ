--Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?
data  =  LOAD  '/niit/h1b'  USING PigStorage('\t')  AS  ( s_no:int, case_status:chararray,employer_name:chararray,  soc_name:chararray, jobtitle:chararray, full_time_position:chararray, wage:long, year:chararray, worksite:chararray, longitude:long, latitude:long );
--dump data
c = group data by employer_name;
--dump c;
--describe c;
g = foreach c generate group as employer, (float)COUNT(data.case_status) as aa;
--dump g;
e = filter g by aa>=1000;
a = filter data by case_status=='CERTIFIED' or case_status=='CERTIFIED-WITHDRAWN';
--dump a;
v = group a by employer_name;
--dump v;
b= foreach v generate group as employer, (float)COUNT(a.case_status) as count2;
--dump b;
d = join e by $0, b by $0 ;
--dump d;
e= foreach d generate $0,$1 as g,$3 as b;
--dump e;
f = foreach e generate $0, $1,(b*100)/g as rate;
--dump f;
q = filter f by rate>70;
--dump q;
q1 = order q by rate desc;
dump q1;

--qq = store q into '/home/hduser/output_Proj9';
