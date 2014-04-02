/*
Register the piggybank jar file to be able to use the UDFs in it
*/
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

/*
Load the logs dataset using piggybank's CombinedLogLoader into the relation logs
*/
logs = LOAD '/user/cloudera/pdp/datasets/logs/access.log'
       USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader()
       AS (addr: chararray, logname: chararray, user: chararray, time: chararray,
           method: chararray, uri: chararray, proto: chararray,
           status: int, bytes: int,
           referer: chararray, useragent: chararray);
/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the relation logs on the console
DUMP logs;