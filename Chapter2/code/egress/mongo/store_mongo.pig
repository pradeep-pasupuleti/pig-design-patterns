/*
Register the mongo jar files and piggybank jar to be able to use the UDFs
*/
REGISTER '/home/cloudera/pdp/jars/mongo.jar';
REGISTER '/home/cloudera/pdp/jars/mongo_hadoop_pig.jar';
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

/*
Assign the alias MongoStorage to MongoStorage class
*/
DEFINE MongoStorage com.mongodb.hadoop.pig.MongoStorage(); 

/*
Load the contents of files starting with NASDAQ_daily_prices_ into a Pig relation stock_data
*/
stock_data= LOAD '/user/cloudera/pdp/datasets/mongo/NASDAQ_daily_prices/NASDAQ_daily_prices_*' USING org.apache.pig.piggybank.storage.CSVLoader() as (exchange:chararray, stock_symbol:chararray, date:chararray, stock_price_open:chararray, stock_price_high:chararray, stock_price_low:chararray, stock_price_close:chararray, stock_volume:chararray, stock_price_adj_close:chararray);

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

/*
Store data to MongoDB by specifying the MongoStorage serializer.  The MongoDB URI nasdaqDB.store_stock is the document collection created to hold this data.
*/
STORE stock_data INTO 'mongodb://slave1/nasdaqDB.store_stock' using MongoStorage(); 
