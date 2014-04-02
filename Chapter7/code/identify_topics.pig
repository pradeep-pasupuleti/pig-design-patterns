/*
Register piggybank jar file
*/
REGISTER '/home/cloudera/pig-0.11.0/contrib/piggybank/java/piggybank.jar';

/*
*Ideally the following data pre-processing steps have to be generally performed on the actual data, we have deliberately omitted the implementation as these steps were covered in the respective chapters

*Data Ingestion to ingest data from the required sources

*Data Profiling by applying statistical techniques to profile data and find data quality issues

*Data Validation to validate the correctness of the data and cleanse it accordingly

*Data Transformation to apply transformations on the data.

*Data Reduction to obtain a reduced representation of the data.
*/


/*
We have deliberately omitted the steps for vector conversion as we have an example illustrating these in the chapter Understanding Data Reduction. 
*/

/*
Use sh command to execute shell commands.
Convert the files in a directory to sequence files
-i specifies the input directory on HDFS
-o specifies the output directory on HDFS
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout seqdirectory -i /user/cloudera/pdp/datasets/advanced_patterns/lda -o /user/cloudera/pdp/output/advanced_patterns/lda/sequence_files

/*
Create sparse vectors
-i specifies the input directory on HDFS
-o specifies the output directory on HDFS
-nv to get the named vectors
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout seq2sparse -i /user/cloudera/pdp/output/advanced_patterns/lda/sequence_files -o /user/cloudera/pdp/output/advanced_patterns/lda/sparse_vectors -nv -wt tf

/*
Use rowid to convert the sparse vectors by changing the text key to integer
-i specifies the input directory on HDFS
-o specifies the output directory on HDFS
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout rowid -i /user/cloudera/pdp/output/advanced_patterns/lda/sparse_vectors/tf-vectors/ -o /user/cloudera/pdp/output/advanced_patterns/lda/matrix

/*
Use Collapsed Variational Bayes for topic modelling
-i specifies the input directory on HDFS
-o specifies the output directory on HDFS
-k specifies the number of topics
-x specifies the maximum number of iterations
-dict specifies the path to term dictionary
-dt specifies the path to document topic distribution
-mt specifies temporary directory of the model, this is useful when restarting the jobs
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout cvb -i /user/cloudera/pdp/output/advanced_patterns/lda/matrix/matrix -o /user/cloudera/pdp/output/advanced_patterns/lda/lda-out -k 2 -x 5 -dict /user/cloudera/pdp/output/advanced_patterns/lda/sparse_vectors/dictionary.file-* -dt /user/cloudera/pdp/output/advanced_patterns/lda/lda-topics -mt /user/cloudera/pdp/output/advanced_patterns/lda/lda-model

/*
Display top ten words along with their probabilities for each topic
-i specifies the input directory on HDFS
-d specifies the path to the dictionary file
-dt specifies the type of the dictionary (sequence / text)
-sort sorts the Key/Value pairs in descending order
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout vectordump -i /user/cloudera/pdp/output/advanced_patterns/lda/lda-out -d /user/cloudera/pdp/output/advanced_patterns/lda/sparse_vectors/dictionary.file-* -dt sequencefile -vs 10 -sort /user/cloudera/pdp/output/advanced_patterns/lda/lda-out
