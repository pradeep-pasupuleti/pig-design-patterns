/*
Import macro defined in the file pattern_matching_macro.pig
*/
IMPORT '/home/cloudera/pdp/Chapter3/code/pattern_matching_macro.pig';

/*
Load the dataset transactions.csv into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_profiling/transactions.csv' USING  PigStorage(',') AS (transaction_id:long, transaction_date:datetime, cust_id:long, age:chararray, area:chararray, prod_subclass:int, prod_id:long, amt:int, asset:int, sales_price:int, phone_no:chararray, country_code:chararray);

/*
Invoke the macro and pass the relation transactions and the column phone_no as parameters to it.
The pattern matching is performed on the column that is passed.
This macro returns the phone number pattern, its count and the percentage
*/
result = getPatterns(transactions, 'phone_no');

/*
Split the relation result into the relation valid_pattern if the phone number pattern matches any of the two regular expressions. The patterns that do not match any of the regex are stored into the relation invalid_patterns
*/
SPLIT result INTO valid_patterns IF (phone_number MATCHES '([0-9]{3}-[0-9]{3}-[0-9]{4})' or phone_number MATCHES '([0-9]{10})'), invalid_patterns OTHERWISE;

/*
The results are stored on the HDFS in the directories valid_patterns and invalid_patterns
*/
STORE valid_patterns INTO '/user/cloudera/pdp/output/data_profiling/pattern_matching/valid_patterns';
STORE invalid_patterns INTO '/user/cloudera/pdp/output/data_profiling/pattern_matching/invalid_patterns';
