-- Register piggybank jar
REGISTER '/home/cloudera/pig-0.11.0/contrib/piggybank/java/piggybank.jar';

/*
XMLLoader accesses the specified XML file and retrieves the record level value to be stored in the tuple data specified by the parameter to the XMLLoader.
*/
data = LOAD '/user/cloudera/pdp/datasets/xml/mplus_topics_2013-09-26.xml' USING org.apache.pig.piggybank.storage.XMLLoader('article');

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Print the contents of the relation data to the console
*/
DUMP data;
