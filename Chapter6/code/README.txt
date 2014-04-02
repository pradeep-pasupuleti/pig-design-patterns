Below are the steps to execute K-means clustering, we have used Mahout 0.7 for K-means clustering
Set mahout jars to the environment variable HADOOP_CLASSPATH, alternatively these can be specified using libjars option. Substitute the specific version number for *
$ export HADOOP_CLASSPATH=$MAHOUT_HOME/mahout-core-*.jar:$MAHOUT_HOME/mahout-math-*.jar:/your/path/to/hadoop_core/hadoop-core*.jar:/your/path/to/hadoop_mapred/hadoop-mapred-*.jar:/your/path/to/log4j/log4j-*.jar:your/path/to/google_collections/google-collections-*.jar:$MAHOUT_HOME/lib/guava-*.jar:$CLASSPATH

Execute the Pig script gen_seq_file.pig using the below command.
$ pig gen_seq_file.pig

Use the below command to execute the shell script execute_kmeans.sh available in the folder shell_scripts.
$ sh execute_kmeans.sh


Below are the steps to execute dimensionality reduction using SSVD on Mahout, we have used Mahout 0.8 for this.
Set mahout jars to the environment variable HADOOP_CLASSPATH, alternatively these can be specified using libjars option. Substitute the specific version number for *
$ export HADOOP_CLASSPATH=$MAHOUT_HOME/core/target/mahout-core-*.jar:$MAHOUT_HOME/math/target/mahout-math-*.jar:/your/path/to/hadoop_core/hadoop-core-*.jar:/your/path/to/hadoop_mapred/hadoop-mapred-*.jar:/your/path/to/log4j/log4j-*.jar:your/path/to/google_collections/google-collections-*.jar:$MAHOUT_HOME/lib/guava-*.jar:$CLASSPATH

Use the below command to execute the shell script csv_to_sequence.sh available in the folder shell_scripts.
$ sh csv_to_sequence.sh

Execute the Pig script pca_ssvd.pig using the below command.
$ pig pca_ssvd.pig


Below are the prerequisites for executing pca.pig (the Pig script calls R script via streaming)
R should be installed on all the nodes of the Hadoop cluster.
The Rscript compute_pca.R should have execute permissions on it.