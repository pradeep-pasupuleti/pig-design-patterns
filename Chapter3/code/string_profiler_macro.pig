/*
Define the macro, specify the input parameters and the return value
*/
DEFINE getStringProfile(data,columnName) returns string_profile{

/*
Calculate row count and null count on the column country_code
*/
data_grpd = GROUP $data ALL;
string_stats = FOREACH data_grpd  
{
	rowCount = COUNT_STAR($data.$columnName);
	nullCount = COUNT($data.$columnName);
 	GENERATE 'Column Name','$columnName' AS colName,'Row Count',rowCount,'Null Count' , (rowCount - nullCount),'Distinct Values' as dist,'NONE' as distvalue;
}

/*
Calculate total char count, max chars, min chars, avg chars on the column country_code
*/
size = FOREACH $data GENERATE SIZE($columnName) AS chars_count;
size_grpd_all = GROUP size ALL;
char_stats = FOREACH size_grpd_all GENERATE 'Total Char Count',SUM(size.chars_count) AS total_char_count,'Max Chars', MAX(size.chars_count) AS max_chars_count,'Min Chars', MIN(size.chars_count) AS min_chars_count,'Avg Chars', AVG(size.chars_count) AS avg_chars_count,'Distinct Values' as dist,'NONE' as distvalue;

/*
Calculate total word count, max words and min words on the column country_code
*/
words = FOREACH $data GENERATE FLATTEN(TOKENIZE($columnName)) AS word;
whitespace_filtrd_words = FILTER words BY word MATCHES '\\w+';
grouped_words = GROUP whitespace_filtrd_words BY word;
word_count = FOREACH grouped_words GENERATE COUNT(whitespace_filtrd_words) AS count, group AS word;
word_count_grpd_all = GROUP word_count ALL;
words_stats = FOREACH word_count_grpd_all GENERATE 'Word Count',SUM(word_count.count) AS total_word_count, 'Max Words',MAX(word_count.count) AS max_count, 'Min Words',MIN(word_count.count) AS min_count,'Distinct Values' as dist,'NONE' as distvalue;

/*
Identify distinct country codes and their count
*/
grpd_data = GROUP $data BY $columnName;
grpd_data_count = FOREACH grpd_data GENERATE group as country_code, COUNT($data.$columnName) AS country_count;

/*
Calculate the total sum of all the counts
*/
grpd_data_cnt_all = GROUP grpd_data_count ALL;
total_count = FOREACH grpd_data_cnt_all GENERATE SUM(grpd_data_count.country_count) AS tot_sum;

/*
Calculate the percentage of the distinct country codes
*/
percentage = FOREACH grpd_data_count GENERATE country_code as country_code, 
country_count as country_code_cnt, ROUND(country_count*100.0/total_count.tot_sum) as percent,'Distinct Values' as dist;

/*
Join string stats, char_stats, word_stats and the relation with distinct country codes, their count and the rounded percentage. Return these values
*/
percentage_grpd = GROUP percentage BY dist;
$string_profile = JOIN string_stats BY dist,char_stats BY dist , words_stats BY dist, percentage_grpd BY group;
};