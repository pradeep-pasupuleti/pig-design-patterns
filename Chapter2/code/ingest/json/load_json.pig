/*
Use JSONLoader UDF, it takes in the parameter of the JSON schema and loads the contents of the JSON file emails.json into a map enron_emails
*/
enron_emails = LOAD '/user/cloudera/pdp/datasets/json/emails.json' USING JsonLoader('body:chararray, from:chararray, tos:chararray, ccs:chararray, bccs:chararray, date:chararray, message_id:chararray, subject:chararray');

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Display the contents of the relation enron_emails on the console
*/
DUMP enron_emails;