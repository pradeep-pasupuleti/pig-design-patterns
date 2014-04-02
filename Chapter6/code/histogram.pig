/*
Register the custom UDF
*/
REGISTER '/home/cloudera/pdp/jars/databucketgenerator.jar';

/*
Define the alias generateBuckets for the custom UDF, the number of buckets(20) is passed as a parameter
*/
DEFINE generateBuckets com.datareduction.GenerateBuckets('20');

/*
Load the data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_reduction/transactions.csv' USING  PigStorage(',') AS (transaction_id:long,transaction_date:chararray, cust_id:chararray, age:chararray, area:chararray, prod_subclass:int, prod_id:long, quantity:int, asset:int, transaction_amt:double, phone_no:chararray, country_code:chararray);

/*
Maximum value of transactions amount and the actual transaction amount are passed to generateBuckets UDF
The UDF calculates the bucket size by dividing maximum transaction amount by the number of buckets.
It finds out the range to which each value belongs to and returns the value along with the bucket range
*/
transaction_amt_grpd = GROUP transactions ALL;
transaction_amt_min_max = FOREACH transaction_amt_grpd GENERATE MAX(transactions.transaction_amt) AS max_transaction_amt,FLATTEN(transactions.transaction_amt) AS transaction_amt;
transaction_amt_buckets = FOREACH transaction_amt_min_max GENERATE generateBuckets(max_transaction_amt,transaction_amt) ;

/*
Calculate the count of values in each range
*/
transaction_amt_buckets_grpd = GROUP transaction_amt_buckets BY range;
transaction_amt_buckets_count = FOREACH transaction_amt_buckets_grpd GENERATE group, COUNT(transaction_amt_buckets);

/*
The results are stored on HDFS in the directory histogram
*/
STORE transaction_amt_buckets_count INTO '/user/cloudera/pdp/output/data_reduction/histogram';
