/*
Define alias for the UDF BuildBloom.
The first parameter to BuildBloom constructor is the hashing technique to use, the second parameter specifies the number of distinct elements that would be placed in the filter and the third parameter is the acceptable rate of false positives.
*/
DEFINE BuildBloom BuildBloom('jenkins', '75000', '0.1');

/*
Load dictionary words
*/
dict_words1 = LOAD '/user/cloudera/pdp/datasets/data_validation/unstructured_text/dictionary_words1.csv' as (words:chararray); 
dict_words2 = LOAD '/user/cloudera/pdp/datasets/data_validation/unstructured_text/dictionary_words2.csv' as (words:chararray);

/*
Load stop words
*/
stop_words_list = LOAD '/user/cloudera/pdp/datasets/data_validation/unstructured_text/stopwords.txt' USING PigStorage();
stopwords = FOREACH stop_words_list GENERATE FLATTEN(TOKENIZE($0));

/*
Load the document corpus and tokenize to extract the words
*/
doc1 = LOAD '/user/cloudera/pdp/datasets/data_validation/unstructured_text/computer_science.txt' AS (words:chararray);
docWords1 = FOREACH doc1 GENERATE FLATTEN(TOKENIZE(words)) AS word;
doc2 = LOAD '/user/cloudera/pdp/datasets/data_validation/unstructured_text/information_technology.txt' AS (words:chararray);
docWords2 = FOREACH doc2 GENERATE FLATTEN(TOKENIZE(words)) AS word;

/*
Combine the contents of the relations docWords1 and docWords2
*/
combined_docs = UNION docWords1, docWords2;

/*
Convert to lowercase, remove stopwords, punctuations, spaces, numbers.
Replace nulls with the value "dummy string"
*/
lowercase_data = FOREACH combined_docs GENERATE FLATTEN(TOKENIZE(LOWER($0))) as word;
joind = JOIN stopwords BY $0 RIGHT OUTER, lowercase_data BY $0;
stop_words_removed = FILTER joind BY $0 IS NULL;
punctuation_removed = FOREACH stop_words_removed	
{
	replace_punct = REPLACE($1,'[\\p{Punct}]','');
	replace_space = REPLACE(replace_punct,'[\\s]','');
	replace_numbers = REPLACE(replace_space,'[\\d]','');
	GENERATE replace_numbers AS replaced_words;
}
replaced_nulls = FOREACH punctuation_removed GENERATE (SIZE($0) > 0 ? $0 : 'dummy string') as word;

/*
Remove duplicate words
*/
unique_words_corpus = DISTINCT replaced_nulls;

/*
Combine the two relations containing dictionary words
*/
dict_words = UNION dict_words1, dict_words2;

/*
BuildBloom builds a bloom filter that will be used in Bloom.
Bloom filter is built on the relation dict_words which contains all the dictionary words.
The resulting file dict_words_bloom is used in bloom filter by passing it to Bloom.
The call to bloom returns the words that are present in the dictionary, we select the words that are not present in the dictionary and classify them as misspelt words. The misspelt words are filtered from the original dataset and are stored in the folder invalid_data.
*/
dict_words_grpd = GROUP dict_words all;
dict_words_bloom = FOREACH dict_words_grpd GENERATE BuildBloom(dict_words.words);
STORE dict_words_bloom into 'dict_words_bloom';
DEFINE bloom Bloom('dict_words_bloom');
filterd = FILTER unique_words_corpus BY NOT(bloom($0));
joind = join filterd by $0, unique_words_corpus by $0;
joind_right = join filterd by $0 RIGHT, unique_words_corpus by $0;
valid_words_filter = FILTER joind_right BY $0 IS NULL;
valid_words = FOREACH valid_words_filter GENERATE $1;
misspellings = FOREACH joind GENERATE $0 AS misspelt_word;

/*
The results are stored on the HDFS in the directories valid_data and invalid_data.
The misspelt words are written to a file in the folder invalid_data.
*/
STORE misspellings INTO '/user/cloudera/pdp/output/data_validation_cleansing/unstructured_data_validation_cleansing/invalid_data';
STORE valid_words INTO '/user/cloudera/pdp/output/data_validation_cleansing/unstructured_data_validation_cleansing/valid_data';
