-- Register piggybank jar
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

-- Register Avro and JSON jar files
REGISTER '/home/cloudera/pdp/jars/avro-1.7.4.jar';
REGISTER '/home/cloudera/pdp/jars/json-simple-1.1.1.jar';

/*
Assign the alias AvroStorage to piggybank's AvroStorage UDF
*/
DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

/*
Load the dataset using the alias AvroStorage into the relation health_topics
*/
health_topics = LOAD '/user/cloudera/pdp/datasets/xml/mplus-topics_2013-09-26.avro' USING AvroStorage;

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Print the contents of the relation health_topics to the console
DUMP health_topics;
