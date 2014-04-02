/*
Register the datafu and custom storage jar files
*/
REGISTER '/home/cloudera/pdp/jars/datafu.jar';
REGISTER '/home/cloudera/pdp/jars/customprofilestorage.jar';

/*
Import macro defined in the file numerical_profiler_macro.pig
*/
IMPORT '/home/cloudera/pdp/Chapter3/code/numerical_profiler_macro.pig';

/*
Load the transactions data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_profiling/transactions.csv' USING  PigStorage(',') AS (transaction_id:long, transaction_date:datetime, cust_id:long, age:chararray, area:chararray, prod_subclass:int, prod_id:long, amt:int, asset:int, sales_price:int, phone_no:chararray, country_code:chararray);

/*
Use SAMPLE operator to pick a subset of the data, at most 20% of the data is returned as a sample
*/
sample_transactions = SAMPLE transactions 0.2;

/*
Invoke the macro getProfile with the parameters sample_transactions which contains a sample of the data set and the column name on which the numerical profiling has to be done.
The macro performs numerical profiling on the sales_price column and returns various statistics like variance, standard deviation, row count, null count, distinct count and mode
*/
result =  getProfile(sample_transactions,'sales_price');

/*
CustomProfileStorage UDF extends the StoreFunc. All the abstract methods have been overridden to implement logic that writes the contents of the relation into a file in a custom report like format.
The results are stored on the HDFS in the directory numeric
*/
STORE result INTO '/user/cloudera/pdp/output/data_profiling/numeric' USING com.profiler.CustomProfileStorage();