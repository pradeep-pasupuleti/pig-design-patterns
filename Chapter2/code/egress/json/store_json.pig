/*
Load the json file using JsonLoader to the relation enron_emails
*/
enron_emails = LOAD '/user/cloudera/pdp/datasets/json/emails.json' USING JsonLoader('body:chararray, from:chararray, tos:chararray, ccs:chararray, bccs:chararray, date:chararray, message_id:chararray, subject:chararray');

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Use JsonStorage to store the contents of the relation to a json file
*/
STORE enron_emails into '/user/cloudera/pdp/output/json/output.json' USING JsonStorage();
