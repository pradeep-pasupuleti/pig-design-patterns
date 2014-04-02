/*
Load the production datasets of five manufacturing units into the relations
*/
production_unit_1 = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_unit_1.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);
production_unit_2 = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_unit_2.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);
production_unit_3 = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_unit_3.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);
production_unit_4 = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_unit_4.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);
production_unit_5 = LOAD '/user/cloudera/pdp/datasets/data_transformation/production_unit_5.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);

/*
Combine the data in the relations using UNION operator
*/
production = UNION production_unit_1,production_unit_2,production_unit_3,production_unit_4,production_unit_5;

/*
Load manufacturing_unit and manufacturing_units_products datasets
*/
manufacturing_units_products = LOAD '/user/cloudera/pdp/datasets/data_transformation/manufacturing_units_products.csv' USING PigStorage(',') AS (manufacturing_unit_id:chararray,product_id:chararray,capacity_per_hour:int,manufacturing_cost:float);
manufacturing_units = LOAD '/user/cloudera/pdp/datasets/data_transformation/manufacturing_units.csv' USING PigStorage(',') AS (manufacturing_unit_id:chararray,manufacturing_unit_name:chararray,manufacturing_unit_city:chararray,country:chararray,currency:chararray);

/*
Use replicated join to join the relation production, which is huge with a smaller relation manufacturing_units_products.
The relations manufacturing_units_products and manufacturing units are small enough to fit into the memory
*/
replicated_join = JOIN production BY (manufacturing_unit_id,product_id), manufacturing_units_products BY (manufacturing_unit_id,product_id) USING 'replicated';
manufacturing_join = JOIN replicated_join BY production::manufacturing_unit_id, manufacturing_units BY manufacturing_unit_id USING 'replicated';

/*
Identify varying representation of currency and transform the values in the attribute manufacturing_cost to USD for the units that have INR as currency
*/
transformed_varying_values = FOREACH manufacturing_join GENERATE $0 AS production_date,$2 AS manufacturing_unit_id,$3 AS product_id,$4 AS actual_quantity_produced,
($1*$7) AS expected_quantity_produced,(float)((($13 == 'INR') ? ($8/60) : $8)*$4) AS manufacturing_cost;

/*
Calculate the expected quantity to be produced, actual quantity produced, percentage, total manufacturing cost for each month for each manufacturing unit and product to identify how each unit is performing
*/
transformed_varying_values_grpd = GROUP transformed_varying_values BY (GetMonth($0),manufacturing_unit_id,product_id);
quantity_produced = FOREACH transformed_varying_values_grpd 
{
	expected_quantity_produced = SUM(transformed_varying_values.expected_quantity_produced);
	actual_quantity_produced = SUM(transformed_varying_values.actual_quantity_produced);
	percentage_quantity_produced = 100*actual_quantity_produced/expected_quantity_produced;
	manufacturing_cost = SUM(transformed_varying_values.manufacturing_cost);
	GENERATE group.$0 AS production_month,group.$1 AS manufacturing_unit_id,group.$2 AS product_id,expected_quantity_produced AS expected_quantity_produced,actual_quantity_produced AS actual_quantity_produced,percentage_quantity_produced AS percentage_quantity_produced,ROUND(manufacturing_cost) AS manufacturing_cost;
}

/*
Sort the relation by the percentage of quantity produced
*/
ordered_quantity_produced = ORDER quantity_produced BY $5 DESC;

/*
The results are stored on the HDFS in the directory data_integration
*/
STORE ordered_quantity_produced INTO '/user/cloudera/pdp/output/data_transformation/data_integration';