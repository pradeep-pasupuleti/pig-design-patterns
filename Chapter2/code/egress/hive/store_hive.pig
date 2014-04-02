-- Register piggybank and hcatalog-pig-adapter jars
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';
REGISTER '/usr/lib/hcatalog/share/hcatalog/hcatalog-pig-adapter.jar';

/*
Load the transactions dataset using PigStorage into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/hive/retail_transactions.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (transaction_no:int, transaction_date:chararray, cust_no:int, amount:double, category:chararray, product:chararray, city:chararray, state:chararray, spendby:chararray);

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Specify the Hive table name transactions_db.retail_transactions as the input to the HCatStorer function.
The contents of the relation transactions are stored into the Hive table.
*/
STORE transactions INTO 'transactions_db.retail_transactions' using org.apache.hcatalog.pig.HCatStorer();
