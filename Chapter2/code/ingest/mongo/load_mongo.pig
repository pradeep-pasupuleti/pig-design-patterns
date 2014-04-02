/*
Register the mongo jar files to be able to use MongoLoader UDF
*/
REGISTER '/home/cloudera/pdp/jars/mongo.jar';    
REGISTER '/home/cloudera/pdp/jars/mongo-hadoop-pig.jar';

/*
Load the data using MongoLoader UDF, it connects to MongoDB, loads the native file and parses it to retrieve only the specified schema.
*/
stock_data = LOAD 'mongodb://slave1/nasdaqDB.store_stock' USING com.mongodb.hadoop.pig.MongoLoader('exchange:chararray, stock_symbol:chararray, date:chararray, stock_price_open:float, stock_price_high:float, stock_price_low:float, stock_price_close:float, stock_volume:long, stock_price_adj_close:chararray') AS (exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close);

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Display the contents of the relation stock_data on the console
*/
DUMP stock_data;
