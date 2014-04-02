/*
Specify the table name as the input to the HCatLoader function provided by HCatalog.
This function abstracts the storage location, files type, schema from the user and takes only the table name as input
*/
transactions = LOAD 'transactions_db.retail_transactions' USING org.apache.hcatalog.pig.HCatLoader(); 

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Display the contents of the relation transactions on the console
*/
DUMP transactions;
