/*
Register Datafu and custom jar files
*/
REGISTER '/home/cloudera/pdp/jars/datatypevalidationudf.jar';
REGISTER  '/home/cloudera/pdp/jars/datafu.jar';

/*
Define aliases for Quantile UDF from Datafu and custom UDF DataTypeValidationUDF.
The parameters to Quantile constructor specify list of quantiles to compute
The parameter to the DataTypeValidationUDF constructor specifies the Data type that would be used for validation
*/
DEFINE Quantile datafu.pig.stats.Quantile('0.25','0.5','0.75'); 
DEFINE DataTypeValidationUDF com.validation.DataTypeValidationUDF('double');

/*
Load automobile insurance claims data set into the relation claims and policy master data set into the relation policy_master
*/
claims = LOAD '/user/cloudera/pdp/datasets/data_validation/automobile_insurance_claims.csv' USING  PigStorage(',') AS (claim_id:chararray, policy_master_id:chararray, registration_no:chararray, engine_no:chararray, chassis_no:chararray, customer_id:int,age:int,first_name:chararray, last_name:chararray,street:chararray,address:chararray,	city:chararray,	zip:long,gender:chararray, claim_date:chararray, garage_city:chararray,bill_no:long,claim_amount:chararray,garage_name:chararray,claim_status:chararray);
policy_master = LOAD '/user/cloudera/pdp/datasets/data_validation/automobile_policy_master.csv' USING  PigStorage(',') AS (policy_master_id:chararray, model:int, make:chararray, price:double, premium:float);

/*
Remove duplicate tuples from the relation claims to ensure that the data meets unique constraint
*/
claims_distinct = DISTINCT claims;

/*
Invoke the custom DataTypeValidationUDF with the parameter claim_amount.
The UDF returns the tuples where claim_amount does not match the specified data type (double), these values are considered as invalid.
Invalid values are stored in the relation invalid_claims_amt
*/
claim_distinct_claim_amount = FOREACH claims_distinct GENERATE claim_amount AS claim_amount;
invalid_c_amount = FOREACH claim_distinct_claim_amount GENERATE DataTypeValidationUDF(claim_amount) AS claim_amount;
invalid_claims_amt = FILTER invalid_c_amount BY claim_amount IS NOT NULL;

/*
Filter invalid values from the relation claims_distinct and segregate the valid and invalid claim amount
*/
valid_invalid_claims_amount_join = JOIN invalid_claims_amt BY  claim_amount RIGHT, claims_distinct BY claim_amount;
valid_claims_amount = FILTER valid_invalid_claims_amount_join BY $0 IS NULL;
invalid_claims_amount = FILTER valid_invalid_claims_amount_join BY $0 IS NOT NULL;

/*
For each invalid_claims_amount, generate all the values and specify the reason for considering these values as invalid
*/
invalid_datatype_claims = FOREACH invalid_claims_amount GENERATE $1 AS claim_id,$2 AS policy_master_id, $3 AS registration_no,$4 AS engine_no, $5 AS chassis_no,$6 AS customer_id,$7 AS age, $8 AS first_name,$9 AS last_name, $10 AS street, $11 AS address, $12 AS city, $13 AS zip, $14 AS gender, $15 AS claim_date, $16 AS garage_city,$17 AS bill_no, $18 AS claim_amount,$19 AS garage_name, $20 AS claim_status,'Invalid Datatype for claim_amount' AS reason;

valid_datatype_claims = FOREACH valid_claims_amount GENERATE $1 AS claim_id,$2 AS policy_master_id, $3 AS registration_no,$4 AS engine_no, $5 AS chassis_no,$6 AS customer_id,$7 AS age, $8 AS first_name,$9 AS last_name, $10 AS street, $11 AS address, $12 AS city, $13 AS zip, $14 AS gender, $15 AS claim_date, $16 AS garage_city,$17 AS bill_no, $18 AS claim_amount,$19 AS garage_name, $20 AS claim_status;

/*
Compute quantiles using Datafu's Quantile UDF
*/
groupd = GROUP valid_datatype_claims ALL;
quantiles = FOREACH groupd {
sorted = ORDER valid_datatype_claims BY age;
GENERATE Quantile(sorted.age) AS quant;
}

/*
Check for occurrence of null values for the column Age which is a numerical field and for city which is a categorical field.
The nulls in age column are replaced with median and the nulls in city column are replaced with a constant string XXXXX.
*/
claims_replaced_nulls = FOREACH valid_datatype_claims GENERATE $0, $1 ,$2 , $3 ,$4 , $5 ,(int) ($6 is null ? FLOOR (quantiles.quant.quantile_0_5) : $6) AS age, $7, $8 ,$9 , $10 , ($11 is null ? 'XXXXX' : $11) AS city, $12, $13 , $14 , $15 , $16 ,(double)$17 , $18 ,$19;

/*
Ensure Referential integrity by checking if the policy_master_id in the claims dataset is present in the master dataset.
The values in the claims dataset that do not find a match in the master dataset are considered as invalid values and are removed.
*/
referential_integrity_check = JOIN claims_replaced_nulls BY policy_master_id, policy_master BY policy_master_id;
referential_integrity_invalid_data = JOIN policy_master BY policy_master_id RIGHT, claims_replaced_nulls BY policy_master_id;
referential_check_invalid_claims = FILTER referential_integrity_invalid_data BY $0 IS NULL;

/*
For each referential_check_invalid_claims, generate all the values and specify the reason for considering these values as invalid
*/
invalid_referential_claims = FOREACH referential_check_invalid_claims GENERATE  $5 ,$6, $7, $8 ,$9 , $10 , $11, $12, $13 , $14 , $15 , $16 ,$17 , $18 ,$19,$20,  $21 ,(chararray) $22 , $23 ,$24, 'Referential check Failed for policy_master_id' AS reason;

/*
Perform Range validation by checking if the values in the claim_amount column are within a range of 7% to 65% of the price in the master dataset.
The values that fall outside the range are considered as invalid values and are removed.
*/
referential_integrity_valid_claims = FILTER referential_integrity_check BY ( claims_replaced_nulls::claim_amount >= (policy_master::price*7/100) AND claims_replaced_nulls::claim_amount <= (policy_master::price*65/100 ));
valid_claims = FOREACH referential_integrity_valid_claims GENERATE $0, $1 ,$2 , $3 ,$4 , $5 ,$6 , $7, $8 ,$9 , $10 , $11 , $12, $13 , $14 , $15 , $16 ,$17 , $18 ,$19;
invalid_range = FILTER referential_integrity_check BY ( claims_replaced_nulls::claim_amount <= (policy_master::price*7/100) OR claims_replaced_nulls::claim_amount >= (policy_master::price*65/100 ));

/*
For each invalid_range, generate all the values and specify the reason for considering these values as invalid
*/
invalid_claims_range = FOREACH invalid_range GENERATE $0, $1 ,$2 , $3 ,$4 , $5 ,$6, $7, $8 ,$9 , $10 , $11, $12, $13 , $14 , $15 , $16 ,(chararray)$17 , $18 ,$19, 'claim_amount not within range' AS reason;

/*
Combine all the relations containing invalid values. 
*/
invalid_claims = UNION invalid_datatype_claims,invalid_referential_claims,invalid_claims_range;

/*
The results are stored on the HDFS in the directories valid_data and invalid_data
The values that are not meeting the constraints are written to a file in the folder invalid_data. This file has an additional column specifying the reason for elimination of the record, this can be used for further analysis.
*/
STORE valid_claims INTO '/user/cloudera/pdp/output/data_validation_cleansing/constraints_validation_cleansing/valid_data';
STORE invalid_claims INTO '/user/cloudera/pdp/output/data_validation_cleansing/constraints_validation_cleansing/invalid_data';