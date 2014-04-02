Before executing the Pig scripts, create a database, table and load data into it, below are the steps to create the database and table in Hive

hive> create database transactions_db;

hive> use transactions_db;

hive> create table retail_transactions(transaction_no INT, transaction_date STRING, cust_no INT, amount DOUBLE,category STRING, product STRING, city STRING, state STRING, spendby STRING) row format delimited Fields terminated by ',';
hive> LOAD DATA INPATH 'hdfs/path/to/dataset/retail_transactions.csv' OVERWRITE INTO TABLE retail_transactions;
hive> create table retail_transactions_rc(transaction_no INT, transaction_date STRING, cust_no INT, amount DOUBLE,category STRING, product STRING, city STRING, state STRING, spendby STRING) Stored as RCFile;
hive> 
INSERT OVERWRITE table retail_transactions_rc SELECT * from retail_transactions;


Use the below flag to bring in the jars for execution with HCatalog
$ pig -useHCatalog <filename.pig>

Alternatively, use the below command to specify the jars in the environment variable PIG_CLASSPATH before executing the script. The specific version number should be substituted for *

$ export PIG_CLASSPATH=$HCAT_HOME/share/hcatalog/hcatalog-core*.jar: $HCAT_HOME/share/hcatalog/hcatalog-pig-adapter*.jar: $HIVE_HOME/lib/hive-metastore-*.jar: $HIVE_HOME/lib/libthrift-*.jar: $HIVE_HOME/lib/hive-exec-*.jar:$HIVE_HOME/lib/libfb303-*.jar: $HIVE_HOME/lib/jdo2-api-*-ec.jar:$HIVE_HOME/conf:$HADOOP_HOME/conf: $HIVE_HOME/lib/slf4j-api-*.jar