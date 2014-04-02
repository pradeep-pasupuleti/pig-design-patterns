/*
Register the Piggybank jar file to be able to use the UDFs in it
*/
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

-- Register Hive common and exec jars
REGISTER '/usr/lib/hive/lib/hive-common-0.11.0.1.3.0.0-107.jar';
REGISTER '/usr/lib/hive/lib/hive-exec-0.11.0.1.3.0.0-107.jar';

/*
Load retail_transactions_rc  RCfile and specifies the names of the columns of the table and their types in the constructor of HiveColumnarLoader.
*/
transactions = LOAD '/apps/hive/warehouse/transactions_db.db/retail_transactions_rc' USING org.apache.pig.piggybank.storage.HiveColumnarLoader('transaction_no int,transaction_date string,cust_no int,amount double,category string,product string,city string,state string,spendby string');

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Display the contents of the relation transactions on the console
*/
DUMP transactions;
