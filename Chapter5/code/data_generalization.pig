/*
Load products_components data set into the relation products_components
*/
products_components = LOAD '/user/cloudera/pdp/datasets/data_transformation/products_components.csv' USING PigStorage(',') AS (product_id:chararray,component_id:chararray,required_qty_per_Unit:int);

/*
Calculate the distinct count for product_id and component_id and store the results in the relations products_unique_count and components_unique_count
*/
products_components_grpd = GROUP products_components ALL;
products_unique_count = FOREACH products_components_grpd
{
	attribute_name = 'Products';
	distinct_prod = DISTINCT products_components.product_id;
	GENERATE attribute_name AS attribute_name, COUNT(distinct_prod) AS attribute_count; 
}
components_unique_count = FOREACH products_components_grpd
{
	attribute_name = 'Components'; 
	distinct_comp = DISTINCT products_components.component_id;
	GENERATE attribute_name AS attribute_name, COUNT(distinct_comp) AS attribute_count; 
}

/*
The relations product_unique_count and components_unique_count are combined using the UNION operator.
This relation contains two columns attribute_name and attribute_count, it is then sorted by attribute_count
*/
combined_products_components_count = UNION products_unique_count,components_unique_count;
ordered_count = ORDER combined_products_components_count BY attribute_count ASC;

/*
The results are stored on the HDFS in the directory data_generalization
*/
STORE ordered_count INTO '/user/cloudera/pdp/output/data_transformation/data_generalization';

