/*
Load the production data set into the relation production
*/
production = LOAD '/user/cloudera/pdp/datasets/data_transformation/production.csv' USING PigStorage(',') AS (production_date:datetime,production_hours:int,manufacturing_unit_id:chararray,product_id:chararray,produced_quantity:int);

/*
Filter the relation products to fetch the records with product id C001
*/
production_filt = FILTER production BY product_id=='C001';

/*
Calculate the total production hours and total produced quantity of product C001 in each manufacturing unit
*/
production_grpd = GROUP production_filt BY (manufacturing_unit_id,product_id);
production_sum = FOREACH production_grpd GENERATE group.$0 AS manufacturing_unit_id, group.$1 AS product_id,(float) SUM(production_filt.production_hours) AS production_hours,(float)SUM(production_filt.produced_quantity) AS produced_quantity;

/*
Apply Min max normalization on total production hours and total produced quantity for each manufacturing unit to scale the data to fit in the range of [0-1]
*/
production_sum_grpd = GROUP production_sum ALL;
production_min_max = FOREACH production_sum_grpd GENERATE MIN(production_sum.production_hours)-1 AS min_hour,MAX(production_sum.production_hours)+1 AS max_hour, MIN(production_sum.produced_quantity)-1 AS min_qty, MAX(production_sum.produced_quantity)+1 AS max_qty;
production_norm = FOREACH production_sum 
{
norm_production_hours = (float)(((production_hours - production_min_max.min_hour)/(production_min_max.max_hour - production_min_max.min_hour))*(1-0))+1;
norm_produced_quantity = (float)(((produced_quantity - production_min_max.min_qty)/(production_min_max.max_qty - production_min_max.min_qty))*(1-0))+1;
GENERATE manufacturing_unit_id AS manufacturing_unit_id, product_id AS product_id, norm_production_hours AS production_hours, norm_produced_quantity AS produced_quantity;
}
prod_norm = FOREACH production_norm GENERATE manufacturing_unit_id AS manufacturing_unit_id,product_id AS product_id,production_hours AS production_hours,produced_quantity AS produced_quantity;

/*
Calculate the Euclidean distance to find out similar manufacturing units w.r.t the product C001
*/
manufacturing_units_euclidean_distance  = FOREACH (CROSS production_norm,prod_norm) {
distance_between_points = (production_norm::production_hours - prod_norm::production_hours)*(production_norm::production_hours - prod_norm::production_hours) + (production_norm::produced_quantity - prod_norm::produced_quantity)*(production_norm::produced_quantity - prod_norm::produced_quantity);
GENERATE  production_norm::manufacturing_unit_id, production_norm::product_id,prod_norm::manufacturing_unit_id, prod_norm::product_id,SQRT(distance_between_points) as dist;         
};

/*
The results are stored on the HDFS in the directory data_normalization
*/
STORE manufacturing_units_euclidean_distance INTO '/user/cloudera/pdp/output/data_transformation/data_normalization';
