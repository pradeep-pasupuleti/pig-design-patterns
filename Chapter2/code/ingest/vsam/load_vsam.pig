/*
Register custom UDF vsamloader.jar and  cb2java jar which is a dynamic COBOL copybook parser for Java
*/
REGISTER '/home/cloudera/pdp/jars/vsamloader.jar';
REGISTER '/home/cloudera/pdp/jars/cb2java0.3.1.jar';

/*
Load the contents of the automobile insurance claims dataset using custom UDF.
VSAMLoader uses the copybook file to parse the data and returns the schema to be used to load the data
*/
data = LOAD '/user/cloudera/pdp/datasets/vsam/automobile_insurance_claims_vsam.csv' USING com.mycustomloader.vsamloader.VSAMLoader();

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the relation data on the console
DUMP data;

-- Display the schema of the relation data
DESCRIBE data;