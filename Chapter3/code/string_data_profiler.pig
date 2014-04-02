/*
Register the datafu and custom storage jar files
*/
REGISTER '/home/cloudera/pdp/jars/datafu.jar';
REGISTER '/home/cloudera/pdp/jars/customprofilestorage.jar';

/*
Import macro defined in the file string_profiler_macro.pig
*/
IMPORT '/home/cloudera/pdp/Chapter3/code/string_profiler_macro.pig';

/*
Load the transactions data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_profiling/transactions.csv' using PigStorage(',') as (transaction_id:long,transaction_date:datetime, cust_id:long, age:chararray, area:chararray, prod_subclass:int, prod_id:long, amt:int, asset:int, sales_price:int, phone_no:chararray, country_code:chararray);

/*
Invoke the macro getStringProfile with the parameters transactions and the column name on which the string profiling has to be done.
The macro performs string profiling on the country_code column and returns various statistics like row count, null count, total character count, word count, identifies distinct country codes in the dataset and calculates their count and percentage.
*/
result =  getStringProfile(transactions,'country_code');

/*
CustomProfileStorage UDF extends the StoreFunc. All the abstract methods have been overridden to implement logic that writes the contents of the relation into a file in a custom report like format.
The results are stored on the HDFS in the directory string
*/
STORE result INTO '/user/cloudera/pdp/output/data_profiling/string' USING com.profiler.CustomProfileStorage();