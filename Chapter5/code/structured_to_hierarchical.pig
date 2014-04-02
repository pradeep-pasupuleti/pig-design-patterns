/*
Register the piggybank jar and generateStoreXml jar, it is a custom storage function which generates an XML representation and stores it
*/
REGISTER '/home/cloudera/pdp/jars/generateStoreXml.jar';
REGISTER '/usr/share/pig/contrib/piggybank/java/piggybank.jar';

/*
Load the production dataset into the relation production_details
*/
production_details = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_all.csv' USING  PigStorage(',') AS (production_date,production_hours,manufacturing_unit_id,manufacturing_unit_name,currency,product_id,product_name,quantity_produced);

/*
Call the custom store function TransformStoreXML to transform the contents into a hierarchical representation i.e XML and to store it in the directory structured_to_hierarchical
*/
STORE production_details INTO '/user/cloudera/pdp/output/data_transformation/structured_to_hierarchical' USING com.xmlgenerator.TransformStoreXML('production_details','production_data');