/*
Load the transactions dataset using PigStorage into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/hbase/transactions.csv' USING PigStorage( ',' ) AS (
    listing_id: chararray,
    transaction_date: chararray,
    customer_id: int,
    age: chararray,
    residence_area: chararray,
    product_subclass: int,
    product_id: long,
    amount: int,
    asset: int,
    sales_price: int);

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Use HBaseStorage to store data from the Pig relation transactions into a HBase table hbase://retail_transactions.
The individual contents of transactions are mapped to three column families transaction_details, product_details and customer_details.
*/
STORE transactions INTO 'hbase://retail_transactions' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('transaction_details:transaction_date customer_details:customer_id customer_details:age customer_details:residence_area product_details:product_subclass product_details:product_id product_details:amount product_details:asset product_details:sales_price');
