/*
Register elephant-bird and JSON jar files
*/
REGISTER '/home/cloudera/pdp/jars/elephant-bird-core-3.0.5.jar';
REGISTER '/home/cloudera/pdp/jars/elephant-bird-pig-3.0.5.jar';
REGISTER '/home/cloudera/pdp/jars/json-simple-1.1.1.jar';

/*
Use ElephantBird’s JSONLoader for loading a nested JSON file
The parameter –nestedload denotes nested loading operation
*/
emails = LOAD '/user/cloudera/pdp/datasets/json/emails.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Display the contents of the relation emails on the console
*/
DUMP emails;
