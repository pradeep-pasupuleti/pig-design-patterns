/*
Load the data from production.csv, manufacturing_units_products.csv, manufacturing_units.csv files into the relations production, manufacturing_units_products and manufacturing_units
The files manufacturing_units_products.csv, manufacturing_units.csv contain master data information.
*/
production = LOAD '/user/cloudera/pdp/datasets/data_transformation/production.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);
manufacturing_units_products = LOAD '/user/cloudera/pdp/datasets/data_transformation/manufacturing_units_products.csv' USING PigStorage(',') AS (manufacturing_unit_id:chararray,product_id:chararray,capacity_per_hour:int,manufacturing_cost:float);
manufacturing_units = LOAD '/user/cloudera/pdp/datasets/data_transformation/manufacturing_units.csv' USING PigStorage(',') AS (manufacturing_unit_id:chararray,manufacturing_unit_name:chararray,manufacturing_unit_city:chararray,country:chararray,currency:chararray);

/*
The relations are joined to get details from the master data.
*/
production_join_manufacturing_units_products = JOIN production BY (manufacturing_unit_id,product_id), manufacturing_units_products BY (manufacturing_unit_id,product_id);
manufacture_join = JOIN production_join_manufacturing_units_products BY production::manufacturing_unit_id, manufacturing_units BY manufacturing_unit_id;

/*
The manufacturing cost attribute is converted to dollars for the units that have currency as INR.
*/
transformed_varying_values = FOREACH manufacture_join GENERATE $2 AS manufacturing_unit_id,$3 AS product_id,GetMonth($0) AS production_month,((($13 == 'INR') ? ($8/60) : $8)*$4) AS manufacturing_cost;

/*
Apply CUBE and ROLLUP aggregations on manufacturing_unit_id, product_id, production_month and store the results in the relations results_cubed and results_rolledup
*/
cubed = CUBE transformed_varying_values BY CUBE(manufacturing_unit_id,product_id,production_month);
rolledup = CUBE transformed_varying_values BY ROLLUP(manufacturing_unit_id,product_id,production_month);
result_cubed = FOREACH cubed GENERATE FLATTEN(group), ROUND(SUM(cube.manufacturing_cost)) AS total_manufacturing_cost;
result_rolledup = FOREACH rolledup GENERATE FLATTEN(group), ROUND(SUM(cube.manufacturing_cost)) AS total_manufacturing_cost;

/*
The results are stored on the HDFS in the directories cube and rollup
*/
STORE result_cubed INTO '/user/cloudera/pdp/output/data_transformation/data_aggregation/cube';
STORE result_rolledup INTO '/user/cloudera/pdp/output/data_transformation/data_aggregation/rollup';
