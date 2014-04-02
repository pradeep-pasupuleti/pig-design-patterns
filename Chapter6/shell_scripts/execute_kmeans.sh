#Create the output directory on HDFS before executing VectorConverter
hadoop fs -mkdir /user/cloudera/pdp/output/data_reduction/kmeans_preproc_nv

#Execute vectorconverter jar to convert the input to named vectors
hadoop jar /home/cloudera/pdp/jars/vectorconverter.jar com.datareduction.VectorConverter /user/cloudera/pdp/output/data_reduction/kmeans_preproc/ /user/cloudera/pdp/output/data_reduction/kmeans_preproc_nv/

#The below Mahout command shows the usage of kmeans. The algorithm takes the input vectors from the path specified in the -i argument, it chooses the initial clusters at random, -k argument specifies the number of clusters as 3, -x specified the maximum number of iterations as 15. -dm specifies the distance measure to use i.e euclidean distance and a convergence threshold specified in -cd as 0.1
/home/cloudera/mahout-distribution-0.7/bin/mahout kmeans -i /user/cloudera/pdp/output/data_reduction/kmeans_preproc_nv/ -c kmeans-initial-clusters -k 3 -o /user/cloudera/pdp/output/data_reduction/kmeans_clusters -x 15 -ow -cl -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -cd 0.01

#Execute cluster dump command to print information about the cluster
/home/cloudera/mahout-distribution-0.7/bin/mahout clusterdump --input /user/cloudera/pdp/output/data_reduction/kmeans_clusters/clusters-x-final --pointsDir /user/cloudera/pdp/output/data_reduction/kmeans_clusters/clusteredPoints --output age_kmeans_clusters
