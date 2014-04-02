/*
Register piggybank jar file
*/
REGISTER '/home/cloudera/pig-0.11.0/contrib/piggybank/java/piggybank.jar';

/*
*The following data pre-processing steps have to be performed here, we have deliberately omitted the implementation as these steps were covered in the respective chapters
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
Generate file descriptor for the training dataset
The string C N 2 C N 2 C N 2 C N C N 2 C N C N 2 C L provides the description of the data.
C specifies that the first attribute is Categorical, it is followed by N specifying the next attribute to be Numeric. This is followed by 2 C which means that the next two attributes are Categorical.
L represents the Label
*/
sh hadoop jar /home/cloudera/mahout-distribution-0.8/core/target/mahout-core-0.8-job.jar org.apache.mahout.classifier.df.tools.Describe -p /user/cloudera/pdp/datasets/advanced_patterns/german-train.data -f /user/cloudera/pdp/datasets/advanced_patterns/german-train.info -d C N 2 C N 2 C N 2 C N C N 2 C N C N 2 C L

/*
Build Random Forests
-t specifies the number of trees to build
-p specifies usage of partial implementation
-sl specifies the number of random attributes to select for each node
-o specifies the output directory
-d specifies the path to training dataset
-ds specifies the data descriptor
-Dmapred.max.split.size indicates the maximum size of each partition
*/
sh hadoop jar /home/cloudera/mahout-distribution-0.8/examples/target/mahout-examples-0.8-job.jar org.apache.mahout.classifier.df.mapreduce.BuildForest -Dmapred.max.split.size=1874231 -d /user/cloudera/pdp/datasets/advanced_patterns/german-train.data -ds /user/cloudera/pdp/datasets/advanced_patterns/german-train.info -sl 5 -p -t 100 -o /user/cloudera/pdp/output/advanced_patterns/classification

/*
Predict the label in the test dataset
-i specifies the file path of the test dataset
-ds specifies the dataset descriptor, we use the one generated for training data as the data description is the same for both training and test data
-m specifies the file path of the decision tree built on the training data
-a specifies that confusion matrix has to be calculated
-mr specifies usage of Hadoop to distribute the classification
-o specifies the output directory
*/
sh hadoop jar /home/cloudera/mahout-distribution-0.8/examples/target/mahout-examples-0.8-job.jar org.apache.mahout.classifier.df.mapreduce.TestForest -i /user/cloudera/pdp/datasets/advanced_patterns/german-test.data -ds /user/cloudera/pdp/datasets/advanced_patterns/german-train.info -m /user/cloudera/pdp/output/advanced_patterns/classification -a -mr -o /user/cloudera/pdp/output/advanced_patterns/classification_pred