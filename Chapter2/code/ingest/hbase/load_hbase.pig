/*
Load data from HBase table retail_transactions, it contains the column families transaction_details, customer_details and product_details.
The : operator is used to access columns in a column family.
First parameter to HBaseStorage is the list of columns and the second parameter is the list of options
The option -loadkey true specifies the rowkey should be loaded as the first item in the tuple, -limit 500 specifies the number of rows to be read from the HBase table
*/
transactions = LOAD 'hbase://retail_transactions'
       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
       'transaction_details:transaction_date customer_details:customer_id customer_details:age customer_details:residence_area product_details:product_subclass product_details:product_id product_details:amount product_details:asset product_details:sales_price', '-loadKey true -limit 500')
       AS (id: bytearray, transaction_date: chararray, customer_id: int, age: chararray, residence_area: chararray, product_subclass: int, product_id: long, amount: int, asset: int, sales_price: int);

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the relation transactions on the console
DUMP transactions;
