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
*/

/*
Use sh command to execute shell commands.
Convert the files in a directory to sequence files
-i specifies the input path of the sequence file on HDFS
-o specifies the output directory on HDFS
-k specifies the rank, i.e the number of dimensions in the reduced space
-us set to true computes the product USigma
-V set to true computes V matrix
-pca set to true runs SSVD in pca mode
*/

sh /home/cloudera/mahout-distribution-0.8/bin/mahout ssvd -i /user/cloudera/pdp/output/data_reduction/ssvd/transactions.seq -o /user/cloudera/pdp/output/data_reduction/ssvd/reduced_dimensions -k 7 -us true -V true -U false -pca true -ow -t 1

/*
Use seqdumper to dump the output in text format.
-i specifies the HDFS path of the input file
*/
sh /home/cloudera/mahout-distribution-0.8/bin/mahout seqdumper -i /user/cloudera/pdp/output/data_reduction/ssvd/reduced_dimensions/V/v-m-00000
