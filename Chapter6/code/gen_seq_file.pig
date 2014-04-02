/*
Register the required jar files
*/
REGISTER '/home/cloudera/pdp/jars/elephant-bird-pig-4.3.jar';
REGISTER '/home/cloudera/pdp/jars/elephant-bird-core-4.3.jar';
REGISTER '/home/cloudera/pdp/jars/elephant-bird-mahout-4.3.jar';
REGISTER '/home/cloudera/pdp/jars/elephant-bird-hadoop-compat-4.3.jar';
REGISTER '/home/cloudera/mahout-distribution-0.7/lib/json-simple-1.1.jar';
REGISTER '/home/cloudera/mahout-distribution-0.7/lib/guava-r09.jar';
REGISTER '/home/cloudera/mahout-distribution-0.7/mahout-examples-0.7-job.jar'; 
REGISTER '/home/cloudera/pig-0.11.0/contrib/piggybank/java/piggybank.jar';

/*
Use declare to create aliases.
declare is a preprocessor statement and is processed before running the script
*/
%declare SEQFILE_LOADER 'com.twitter.elephantbird.pig.load.SequenceFileLoader';
%declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';
%declare VECTOR_CONVERTER 'com.twitter.elephantbird.pig.mahout.VectorWritableConverter';
%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';

/*
Load the data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_reduction/transactions.csv' USING  PigStorage(',') AS (id:long,transaction_date:chararray, cust_id:int, age:int, area:chararray, prod_subclass:int, prod_id:long, quantity:int, asset:int, transaction_amt:double, phone_no:chararray, country_code:chararray);

/*
Extract the columns on which clustering has to be performed
*/
age = FOREACH transactions GENERATE id AS tid, 1 AS index, age AS cust_age;

/*
Generate tuples from the parameters
*/
grpd = GROUP age BY tid;
vector_input = FOREACH grpd generate group, org.apache.pig.piggybank.evaluation.util.ToTuple(age.(index, cust_age));


/*
Use elephant bird functions to store the data into sequence file (mahout readable format)
cardinality represents the dimension of the vector.
*/
STORE vector_input INTO '/user/cloudera/pdp/output/data_reduction/kmeans_preproc' USING $SEQFILE_STORAGE (
 '-c $TEXT_CONVERTER', '-c $VECTOR_CONVERTER -- -cardinality 100'
);
