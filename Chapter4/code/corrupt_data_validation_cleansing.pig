/*
Register Datafu jar file
*/
REGISTER  '/home/cloudera/pdp/jars/datafu.jar';

/*
Define alias for the UDF quantile
The parameters specify list of quantiles to compute
*/
DEFINE Quantile datafu.pig.stats.Quantile('0.25','0.50','0.75'); 

/*
Load automobile insurance claims data set into the relation claims
*/
claims = LOAD '/user/cloudera/pdp/datasets/data_validation/automobile_insurance_claims.csv' USING  PigStorage(',') AS (claim_id:chararray, policy_master_id:chararray, registration_no:chararray, engine_no:chararray, chassis_no:chararray, customer_id:int,age:int,first_name:chararray, last_name:chararray,street:chararray,address:chararray,	city:chararray,	zip:long,gender:chararray, claim_date:chararray, garage_city:chararray,bill_no:long,claim_amount:double,garage_name:chararray,claim_status:chararray);

/*
Sort the relation claims by age
*/
claims_age_sorted = ORDER claims BY age ASC;

/*
Divide the data into equal frequency bins. 
Minimum and maximum values are identified for each bin and are set as bin boundaries.
Replace each bin value with the nearest bin boundary.
*/
bin_id_claims = FOREACH claims_age_sorted GENERATE (customer_id - 1) * 10 / (130- 1 + 1) AS bin_id, $0 ,$1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19 ;
group_by_id = GROUP bin_id_claims BY bin_id;
claims_bin_boundaries = FOREACH group_by_id 
{
	bin_lower_bound =(int) MIN(bin_id_claims.age);
	bin_upper_bound = (int)MAX(bin_id_claims.age);
	GENERATE bin_lower_bound AS bin_lower_bound, bin_upper_bound AS bin_upper_bound, FLATTEN(bin_id_claims);
};
smoothing_by_bin_boundaries = FOREACH claims_bin_boundaries GENERATE $3 AS claim_id,$4 AS policy_master_id,$5 AS registration_no,$6 AS engine_no,$7 AS chassis_no,$8 AS customer_id,( ( $9 - bin_lower_bound ) <= ( bin_upper_bound - $9 ) ? bin_lower_bound : bin_upper_bound ) AS age,$10 AS first_name,$11 AS last_name,$12 AS street,$13 AS address,$14 AS city,$15 AS zip,$16 AS gender,$17 AS claim_date,$18 AS garage_city,$19 AS bill_no,$20 AS claim_amount,$21 AS garage_name,$22 AS claim_status;

/*
Identify outliers present in the column claim_amount by calculating the quartiles, interquartile distance and the upper and lower fences.
The values that do not fall within this range are considered as outliers and are filtered out.
*/
groupd = GROUP smoothing_by_bin_boundaries ALL;
quantiles = FOREACH groupd { 
sorted = ORDER smoothing_by_bin_boundaries BY claim_amount;
GENERATE Quantile(sorted.claim_amount) AS quant;
}
valid_range = FOREACH quantiles GENERATE (quant.quantile_0_25 - 1.5 * (quant.quantile_0_75 - quant.quantile_0_25)) , (quant.quantile_0_75 + 1.5 * (quant.quantile_0_75 - quant.quantile_0_25));
claims_filtered_outliers = FILTER smoothing_by_bin_boundaries BY  claim_amount >= valid_range.$0 AND claim_amount <= valid_range.$1;

/*
Store the invalid values in the relation invalid_claims
*/
invalid_claims_filter = FILTER smoothing_by_bin_boundaries BY  claim_amount <= valid_range.$0 OR claim_amount >= valid_range.$1;
invalid_claims = FOREACH invalid_claims_filter GENERATE $0 ,$1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19,'claim_amount identified as Outlier' as reason;

/*
The results are stored on the HDFS in the directories valid_data and invalid_data
The invalid values are written to a file in the folder invalid_data. This file has an additional column specifying the reason for elimination of the record, this can be used for further analysis.
*/
STORE invalid_claims INTO '/user/cloudera/pdp/output/data_validation_cleansing/corrupt_data_validation_cleansing/invalid_data';
STORE claims_filtered_outliers INTO '/user/cloudera/pdp/output/data_validation_cleansing/corrupt_data_validation_cleansing/valid_data';