/*
Load automobile insurance claims data set into the relation claims
*/
claims = LOAD '/user/cloudera/pdp/datasets/data_validation/automobile_insurance_claims.csv' USING  PigStorage(',') AS (claim_id:chararray, policy_master_id:chararray, registration_no:chararray, engine_no:chararray, chassis_no:chararray, customer_id:int,age:int,first_name:chararray, last_name:chararray,street:chararray,address:chararray,	city:chararray,	zip:long,gender:chararray, claim_date:chararray, garage_city:chararray,bill_no:long,claim_amount:chararray,garage_name:chararray,claim_status:chararray);

/*
Validate the values in the column policy_master_id with a regular expression to match the pattern where the value should start with an alphabet followed by three digits.
The values that do not match the pattern or length are considered as invalid values and are removed.
*/
valid_policy_master_id = FILTER claims BY policy_master_id MATCHES '[aA-zZ][0-9]{3}';

/*
Invalid values are stored in the relation invalid_length
*/
invalid_policy_master_id = FILTER claims BY NOT (policy_master_id MATCHES '[aA-zZ][0-9]{3}');
invalid_length = FOREACH invalid_policy_master_id GENERATE $0, $1 ,$2 , $3 ,$4 , $5 ,$6 , $7, $8 ,$9 , $10 , $11, $12, $13 , $14 , $15 , $16 ,$17 , $18 ,$19,'Invalid length or pattern for policy_master_id' AS reason;

/*
Validate the values in the column claim_date to match MM/DD/YYYY format, also validate the values given for MM and DD to fall within 01 to 12 for month and 01 to 31 for day
The values that do not match the pattern are considered as invalid values and are removed.
*/
valid_claims = FILTER valid_policy_master_id BY ( claim_date MATCHES '^(0?[1-9]|1[0-2])[\\/](0?[1-9]|[12][0-9]|3[01])[\\/]\\d{4}$');

/*
Invalid values are stored in the relation invalid_date
*/
invalid_dates = FILTER valid_policy_master_id BY NOT ( claim_date MATCHES '^(0?[1-9]|1[0-2])[\\/](0?[1-9]|[12][0-9]|3[01])[\\/]\\d{4}$');
invalid_date = FOREACH invalid_dates GENERATE $0, $1 ,$2 , $3 ,$4 , $5 ,$6 , $7, $8 ,$9 , $10 , $11, $12, $13 , $14 , $15 , $16 ,$17 , $18 ,$19,'Invalid date format for claim_date' AS reason;

/*
Combine the relations that contain invalid values. 
*/
invalid_claims = UNION invalid_length,invalid_date;

/*
The results are stored on the HDFS in the directories valid_data and invalid_data
The invalid values are written to a file in the folder invalid_data. This file has an additional column specifying the reason for elimination of the record, this can be used for further analysis.
*/
STORE valid_claims INTO '/user/cloudera/pdp/output/data_validation_cleansing/regex_validation_cleansing/valid_data';
STORE invalid_claims INTO '/user/cloudera/pdp/output/data_validation_cleansing/regex_validation_cleansing/invalid_data';
