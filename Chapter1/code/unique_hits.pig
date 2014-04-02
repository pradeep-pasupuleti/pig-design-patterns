-- Register the jar file to be able to use the UDFs in it
REGISTER 'your_path_to_piggybank/piggybank.jar';

/* Assign aliases ApacheCommonLogLoader, DayMonExtractor, DayExtractor to the CommonLogloader and DateExtractor UDFs
*/
DEFINE ApacheCommonLogLoader org.apache.pig.piggybank.storage.apachelog.CommonLogLoader();
DEFINE DayMonExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('dd/MMM/yyyy:HH:mm:ss Z','dd-MMM');
DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('dd-MMM','dd');

/* Load July and August logs using the alias ApacheCommonLogLoader into the relations raw_logs_Jul and raw_logs_Aug
*/
raw_logs_Jul = LOAD '/user/cloudera/pdp/datasets/logs/NASA_access_logs/Jul/access_log_Jul95' USING ApacheCommonLogLoader AS (jaddr, jlogname, juser, jdt, jmethod, juri, jproto, jstatus, jbytes);
raw_logs_Aug = LOAD '/user/cloudera/pdp/datasets/logs/NASA_access_logs/Aug/access_log_Aug95' USING ApacheCommonLogLoader AS (aaddr, alogname, auser, adt, amethod, auri, aproto, astatus, abytes);

-- Group the two relations by date
jgrpd = GROUP raw_logs_Jul BY DayMonExtractor(jdt);
DESCRIBE jgrpd;
agrpd = GROUP raw_logs_Aug BY DayMonExtractor(adt);
DESCRIBE agrpd;

-- Count the number of unique visits for each day in July
jcountd = FOREACH jgrpd
{
	juserIP =  raw_logs_Jul.jaddr;
	juniqIPs = DISTINCT juserIP; 
	GENERATE FLATTEN(group) AS jdate,COUNT(juniqIPs) AS jcount;
}

-- Count the number of unique visits for each day in August
acountd = FOREACH agrpd
{
	auserIP =  raw_logs_Aug.aaddr;
      auniqIPs = DISTINCT auserIP; 
	GENERATE FLATTEN(group) AS adate,COUNT(auniqIPs) AS acount;
}

-- Display the schema of the relations jcountd and acountd
DESCRIBE jcountd;
DESCRIBE acountd;

/* Join the relations containing count of unique visits in July and August where a match is found for the day of the month
*/
joind = JOIN jcountd BY DayExtractor(jdate), acountd BY DayExtractor(adate);

/* Filter by removing the records where the count is less than 2600
*/
filterd = FILTER joind BY jcount > 2600 and acount > 2600;

/* Debugging operator to understand how the data passes through FILTER and gets transformed
*/
ILLUSTRATE filterd;

/* Sort the relation by date, PARALLEL specifies the number of reducers to be 5
*/
srtd = ORDER filterd BY jdate,adate PARALLEL 5;

-- Limit the number of output records to be 5
limitd = LIMIT srtd 5;

/* Store the contents of the relation into a file in the directory unique_hits_by_month on HDFS
*/
STORE limitd into '/user/cloudera/pdp/output/unique_hits_by_month';