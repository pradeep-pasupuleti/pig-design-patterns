/*
Register custom UDF jar that has a custom storage function XMLStorage to store the data into XML file.
*/
REGISTER '/home/cloudera/pdp/jars/xmlgenerator.jar';

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
Custom UDF XMLStorage generates the XML file and stores it in the xml folder
*/
STORE transactions INTO '/user/cloudera/pdp/output/xml' USING com.xmlgenerator.XMLStorage();