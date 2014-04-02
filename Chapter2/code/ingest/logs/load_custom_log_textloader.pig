/*
Load the logs dataset using TextLoader into the relation logs
*/
logs = LOAD '/user/cloudera/pdp/datasets/logs/sample_log.1' USING TextLoader  AS (line:chararray);

/*
The lines matching the regular expression are stored in parsed_logs.
FILTER function filters the records that do not match the pattern
*/
parsed_logs = FILTER logs BY $0 MATCHES '(Request|Response)(\\s+\\w+)(\\s+\\d+)(\\s+\\d\\d/\\d\\d/\\d\\d\\s+\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d\\s+CST)';

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the relation parsed_logs on the console
DUMP parsed_logs;
