/*
Register datafu and commons math jar files
*/
REGISTER '/home/cloudera/pdp/jars/datafu-1.2.0.jar';
REGISTER '/home/cloudera/pdp/jars/commons-math3-3.2.jar';

/*
Define aliases for the classes SimpleRandomSampleWithReplacementVote and SimpleRandomSampleWithReplacementElect
*/
DEFINE SRSWR_VOTE  datafu.pig.sampling.SimpleRandomSampleWithReplacementVote();
DEFINE SRSWR_ELECT datafu.pig.sampling.SimpleRandomSampleWithReplacementElect();

/*
Load the data set into the relation transactions
*/
transactions= LOAD '/user/cloudera/pdp/datasets/data_reduction/transactions.csv' USING  PigStorage(',') AS (transaction_id:long,transaction_date:chararray, cust_id:chararray, age:int, area:chararray, prod_subclass:int, prod_id:long, quantity:int, asset:int, transaction_amt:double, phone_no:chararray, country_code:chararray);

/*
The input to Vote UDF is the bag of items, the desired sample size (100000 in our use case) and the actual population size.
This UDF votes candidates for each position
*/
summary = FOREACH (GROUP transactions ALL) GENERATE COUNT(transactions) AS count;
candidates = FOREACH transactions GENERATE FLATTEN(SRSWR_VOTE(TOBAG(TOTUPLE(*)), 100000, summary.count));

/*
The Elect UDF elects one candidate for each position and returns a bag of sampled items stored in the relation sampled
*/
sampled = FOREACH (GROUP candidates BY position PARALLEL 10) GENERATE FLATTEN(SRSWR_ELECT(candidates));

/*
The results are stored on the HDFS in the directory sampling
*/
STORE sampled into '/user/cloudera/pdp/output/data_reduction/sampling';