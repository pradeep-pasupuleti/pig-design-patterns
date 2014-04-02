/*
Assign alias ner to the streaming command
Use SHIP to send the streaming binary files (Python script) from the client node to the compute node
*/
DEFINE ner `/home/cloudera/pdp/Chapter7/src/python/named_entities.py` SHIP ('/home/cloudera/pdp/Chapter7/src/python/named_entities.py');

/*
Load the dataset into the relation data
*/
data = LOAD '/user/cloudera/pdp/datasets/advanced_patterns/input.txt';

/*
STREAM is used to send the data to the external script
The result is stored in the relation extracted_named_entities
*/
extracted_named_entities = STREAM data THROUGH ner;

/*
The results are stored on the HDFS in the directory nlp
*/
STORE extracted_named_entities INTO '/user/cloudera/pdp/output/advanced_patterns/nlp';
