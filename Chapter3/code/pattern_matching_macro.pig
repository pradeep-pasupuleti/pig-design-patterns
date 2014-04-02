/*
Define the macro, specify the input parameters and the return value
*/
DEFINE getPatterns(data,phone_no) returns percentage{

/*
Iterate over each row of the phone_no column and transform each value by replacing all digits with 9 and all alphabets with a to form uniform patterns
*/
transactions_replaced = FOREACH $data	
{
	replace_digits = REPLACE($phone_no,'\\d','9');
	replace_alphabets = REPLACE(replace_digits,'[a-zA-Z]','a');
	replace_spaces = REPLACE(replace_alphabets,'\\s','');
 	GENERATE replace_spaces AS phone_number_pattern;
}

/*
Group by phone_number_pattern and calculate count of each pattern
*/
grpd_ph_no_pattern = GROUP transactions_replaced BY phone_number_pattern;
phone_num_count = FOREACH grpd_ph_no_pattern GENERATE group as phone_num, COUNT(transactions_replaced.phone_number_pattern) AS phone_count;

/*
Compute the total count and percentage.
Return the relation percentage with the fields phone number pattern, count and the rounded percentage
*/
grpd_ph_no_cnt_all = GROUP phone_num_count ALL;
total_count = FOREACH grpd_ph_no_cnt_all GENERATE SUM(phone_num_count.phone_count) AS tot_sum;
$percentage = FOREACH phone_num_count GENERATE phone_num as phone_number, phone_count as phone_number_count, CONCAT((Chararray)ROUND(phone_count*100.0/total_count.tot_sum),'%') as percent;
};