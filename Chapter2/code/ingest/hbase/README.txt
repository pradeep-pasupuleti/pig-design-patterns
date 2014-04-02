Before executing the Pig script, create a table in HBase and load the data into it. Below are the steps to create the table.

hbase(main):001:0> create 'retail_transactions','transaction_details','customer_details','product_details'

Execute store_hbase.pig available in the egress folder to load the data into the table retail_transactions.

Ensure that you specify hbase-*.jar and zookeeper.jar in the environment variable PIG_CLASSPATH before executing the script. The specific version number should be substituted for *.