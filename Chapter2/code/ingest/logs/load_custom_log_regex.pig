/*
Register the piggybank jar file to be able to use the UDFs in it
*/
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

/*
Load the logs dataset using piggybank's MyRegExLoader into the relation logs.
MyRegexLoader loads only the lines that match the specified regex format
*/
logs = LOAD '/user/cloudera/pdp/datasets/logs/sample_log.1' 
        USING org.apache.pig.piggybank.storage.MyRegExLoader(
	'(Request|Response)(\\s+\\w+)(\\s+\\d+)(\\s+\\d\\d/\\d\\d/\\d\\d\\s+\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d\\s+CST)') 
	AS (type:chararray, service_name:chararray, req_id:chararray, datetime:chararray);
/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the relation logs on the console
DUMP logs;