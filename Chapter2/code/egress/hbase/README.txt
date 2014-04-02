Before executing the Pig script, create a table in HBase. Below are the steps to create the table.

hbase(main):001:0> create 'retail_transactions','transaction_details','customer_details','product_details'

Ensure that you specify hbase-*.jar and zookeeper.jar in the environment variable PIG_CLASSPATH before executing the script. The specific version number should be substituted for *.