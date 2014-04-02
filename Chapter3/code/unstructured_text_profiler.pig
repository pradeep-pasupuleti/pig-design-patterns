/*
Register our custom text profiler jar
*/
REGISTER '/home/cloudera/pdp/jars/unstructuredtextprofiler.jar';

/*
Load stop words into the relation stop_words_list
*/
stop_words_list = LOAD '/user/cloudera/pdp/datasets/data_profiling/text/stopwords.txt' USING PigStorage();

/*
Tokenize the stopwords to extract the words
*/
stopwords = FOREACH stop_words_list GENERATE FLATTEN(TOKENIZE($0));

/*
Load the dataset into the relations doc1 and doc2.
Tokenize to extract the words for each of these documents
*/
doc1 = LOAD '/user/cloudera/pdp/datasets/data_profiling/text/computer_science.txt' AS (words:chararray);
docWords1 = FOREACH doc1 GENERATE 'computer_science.txt' AS documentId, FLATTEN(TOKENIZE(words)) AS word;
doc2 = LOAD '/user/cloudera/pdp/datasets/data_profiling/text/information_technology.txt' AS (words:chararray);
docWords2 = FOREACH doc2 GENERATE 'information_technology.txt' AS documentId, FLATTEN(TOKENIZE(words)) AS word;

/*
Combine the relations using the UNION operator
*/
combined_docs = UNION docWords1, docWords2;

/*
Perform pre-processing by doing the following
Convert the data into lowercase
Remove stopwords
Perform stemming by calling custom UDF. it uses porter stemmer algorithm to perform stemming
*/
lowercase_data = FOREACH combined_docs GENERATE documentId as documentId, FLATTEN(TOKENIZE(LOWER($1))) as word;
joind = JOIN stopwords BY $0 RIGHT OUTER, lowercase_data BY $1;
stop_words_removed = FILTER joind BY $0 IS NULL;
processed_data = FOREACH stop_words_removed GENERATE documentId as documentId, com.profiler.unstructuredtextprofiling.Stemmer($2) as word;

/*
Calculate word count per word/doc combination using the Group and FOREACH statement and the result is stored in word_count
*/
grpd_processed_data = GROUP processed_data BY (word, documentId);
word_count = FOREACH grpd_processed_data GENERATE group AS wordDoc, 
COUNT(processed_data) AS wordCount;

/*
Calculate Total word count per document using the Group and FOREACH statement and the result is stored in total_docs_wc
*/
grpd_wc = GROUP word_count BY wordDoc.documentId;
grpd_wc_all = GROUP grpd_wc ALL;
total_docs = FOREACH grpd_wc_all GENERATE
FLATTEN(grpd_wc),
COUNT(grpd_wc) AS totalDocs;
total_docs_wc = FOREACH total_docs GENERATE
FLATTEN(word_count),
SUM(word_count.wordCount) AS wordCountPerDoc,
totalDocs;

/*
Calculate Total document count per word is using the Group and FOREACH statement and the result is stored in doc_count_per_word 
*/
grpd_total_docs_wc = GROUP total_docs_wc BY wordDoc.word;
doc_count_per_word = FOREACH grpd_total_docs_wc GENERATE FLATTEN(total_docs_wc),
COUNT(total_docs_wc) AS docCountPerWord;

/*
Calculate tfidf by invoking custom Java UDF.
The overall relevancy of a document with respect to a term is computed and the resultant data is stored in gen_tfidf
*/
gen_tfidf = FOREACH doc_count_per_word GENERATE $0.word AS word,
$0.documentId AS documentId,
com.profiler.unstructuredtextprofiling.GenerateTFIDF(wordCount,
wordCountPerDoc,
totalDocs,
docCountPerWord) AS tfidf;

/*
Order by relevancy
*/
orderd_tfidf = ORDER gen_tfidf BY word ASC, tfidf DESC;

/*
The results are stored on the HDFS in the directory tfidf
*/
STORE orderd_tfidf into '/user/cloudera/pdp/output/data_profiling/unstructured_text_profiling/tfidf';