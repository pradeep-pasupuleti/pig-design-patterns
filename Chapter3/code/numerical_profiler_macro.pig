/*
Define alias VAR for the function datafu.pig.stats.VAR
*/
DEFINE VAR datafu.pig.stats.VAR(); 

/*
Define the macro, specify the input parameters and the return value
*/
DEFINE getProfile(data,columnName) returns numerical_profile{

/*
Calculate the variance, standard deviation, row count, null count and distinct count for the column sales_price
*/
data_grpd = GROUP $data ALL;
numerical_stats = FOREACH data_grpd  
{
	variance = VAR($data.$columnName);
	stdDeviation = SQRT(variance);
	rowCount = COUNT_STAR($data.$columnName);
	nullCount = COUNT($data.$columnName);
	uniq = DISTINCT $data.$columnName;
 	GENERATE 'Column Name','$columnName' AS colName,'Row Count',rowCount,'Null Count' , (rowCount - nullCount), 'Distinct Count',COUNT(uniq),	'Highest Value',MAX($data.$columnName) AS  	max_numerical_count,'Lowest Value',MIN($data.$columnName) 	AS min_numerical_count, 'Total Value',SUM($data.	$columnName) AS total_numerical_count,'Mean Value', AVG	($data.$columnName) AS 	avg_numerical_count,'Variance',variance AS 	variance,'Standard Deviation', stdDeviation AS stdDeviation,'Mode' as modeName,'NONE' as modevalue;
}

/*
Calculate the mode of the column sales_price
*/
groupd = GROUP $data BY $columnName;
groupd_count = FOREACH groupd GENERATE 'Mode' as modeName, group AS mode_values, (long) COUNT($data) AS total;
groupd_count_all = GROUP groupd_count ALL;
frequency = FOREACH groupd_count_all GENERATE MAX(groupd_count.total) AS fq;
filterd = FILTER groupd_count BY (total== frequency.fq AND total>1 AND mode_values IS NOT NULL);
mode  = GROUP filterd BY modeName;

/*
Join relations numerical stats and mode. Return these values
*/
$numerical_profile = JOIN numerical_stats BY modeName FULL,mode BY group;
};
