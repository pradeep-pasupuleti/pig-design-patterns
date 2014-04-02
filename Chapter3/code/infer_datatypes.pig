/*
Register the datatypeinferer and custom storage jar files
*/
REGISTER '/home/cloudera/pdp/jars/datatypeinfererudf.jar';
REGISTER '/home/cloudera/pdp/jars/customdatatypeinfererstorage.jar';

/*
Load the transactions data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_profiling/transactions.csv' USING  PigStorage(',') AS (transaction_id:long, transaction_date:chararray, cust_id:chararray, age:chararray, area:chararray, prod_subclass:int, prod_id:long, amt:int, asset:int, sales_price:int, phone_no:chararray, country_code:chararray);

/*
Infer the data type of the field cust_id by invoking the DataTypeInfererUDF.
It returns a tuple with the inferred data type.
*/
data_types = FOREACH transactions GENERATE com.profiler.DataTypeInfererUDF(cust_id) AS inferred_data_type;

/*
Compute the count of each data type, total count, percentage.
The data type with the highest count is considered as dominant data type
*/
grpd = GROUP data_types BY inferred_data_type;
inferred_type_count = FOREACH grpd GENERATE group AS inferred_type, COUNT(data_types) AS count;
grpd_inf_type_count_all = GROUP inferred_type_count ALL;
total_count = FOREACH grpd_inf_type_count_all GENERATE SUM(inferred_type_count.count) AS tot_sum, MAX(inferred_type_count.count) AS max_val;
percentage = FOREACH inferred_type_count GENERATE inferred_type AS type, count AS total_cnt, CONCAT((Chararray)ROUND(count*100.0/total_count.tot_sum),'%') AS percent,(count==total_count.max_val?'Dominant':'Other') AS inferred_dominant_other_datatype;
percentage_ord = ORDER percentage BY inferred_dominant_other_datatype ASC;

/*
CustomDatatypeInfererStorage UDF extends the StoreFunc. All the abstract methods have been overridden to implement logic that writes the contents of the relation into a file in a custom report like format.
The results are stored on the HDFS in the directory datatype_inferer
*/
STORE percentage_ord INTO '/user/cloudera/pdp/output/data_profiling/datatype_inferer' using com.profiler.CustomDatatypeInfererStorage('cust_id','chararray');