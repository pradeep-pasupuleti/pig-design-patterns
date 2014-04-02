/*
Assign an alias pcar to the streaming command
Use ship to send streaming binary files (R script in this use case) from the client node to the compute node
*/
DEFINE pcar `/home/cloudera/pdp/Chapter6/R_scripts/compute_pca.R` ship('/home/cloudera/pdp/Chapter6/R_scripts/compute_pca.R'); 

/*
Load the data set into the relation transactions
*/
transactions = LOAD '/user/cloudera/pdp/datasets/data_reduction/transactions_multi_dims.csv' USING  PigStorage(',') AS (transaction_id:long, transaction_date:chararray, customer_id:chararray, prod_subclass:chararray, phone_no:chararray, country_code:chararray, area:chararray, product_id:chararray, age:int, amt:int, asset:int, transaction_amount:double, service_rating:int, product_rating:int, curr_stock:int, payment_mode:int, reward_points:int, distance_to_store:int, prod_bin_age:int, cust_height:int);

/*
Extract the columns on which PCA has to be performed.
STREAM is used to send the data to the external script.
The result is stored in the relation princ_components
*/
selected_cols = FOREACH transactions GENERATE age AS age, amt AS amount, asset AS asset, transaction_amount AS transaction_amount, service_rating AS service_rating, product_rating AS product_rating, curr_stock AS current_stock, payment_mode AS payment_mode, reward_points AS reward_points, distance_to_store AS distance_to_store, prod_bin_age AS prod_bin_age, cust_height AS cust_height;
princ_components = STREAM selected_cols THROUGH pcar;

/*
The results are stored on the HDFS in the directory pca
*/
STORE princ_components INTO '/user/cloudera/pdp/output/data_reduction/pca';
