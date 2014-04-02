/*
Assign alias cluster_contacts to the streaming command
Use SHIP to send the streaming binary files (Python script) from the client node to the compute node
*/
DEFINE cluster_contacts `/home/cloudera/pdp/Chapter7/src/python/cluster_contacts.py` SHIP ('/home/cloudera/pdp/Chapter7/src/python/cluster_contacts.py');

/*
Register the piggybank jar file
*/
REGISTER '/home/cloudera/pig-0.11.0/contrib/piggybank/java/piggybank.jar';

/*
Load the outlook_contacts.csv dataset into the relation outlook_contacts
*/
outlook_contacts = LOAD '/user/cloudera/pdp/datasets/advanced_patterns/outlook_contacts.csv' USING PigStorage(',') AS (name: chararray, job_title: chararray);

/*
Transform the job titles by replacing few abbreviations with their full forms
*/
transformed_job_titles = FOREACH outlook_contacts {
job_title_sr = REPLACE(job_title,'Sr', 'Senior');
job_title_jr = REPLACE(job_title_sr,'Jr', 'Junior');
job_title_se = REPLACE(job_title_jr,'SE', 'Software Engineer');
job_title_sse = REPLACE(job_title_se,'SSE', 'Senior Software Engineer');
job_title_md = REPLACE(job_title_sse,'MD', 'Managing Director');
job_title_cs = REPLACE(job_title_md,'CS', 'Company Secretary');
job_title_ceo = REPLACE(job_title_cs,'CEO', 'Chief Executive Officer');
job_title_coo = REPLACE(job_title_ceo,'COO', 'Chief Operations Officer');
job_title_cto = REPLACE(job_title_coo,'CTO', 'Chief Technology Officer');
job_title_cfo = REPLACE(job_title_cto,'CFO', 'Chief Finance Officer');
job_title_vp = REPLACE(job_title_cfo,'VP', 'Vice President');

GENERATE name AS name,job_title_vp AS job_title;
}

/*
Trim spaces for the field job_title
*/
jt_trimmed = FOREACH transformed_job_titles GENERATE TRIM(job_title) AS job_title,name;

/*
Group outlook_contacts by job_title
Extract unique job titles and store into the relation jt_flattened
STREAM is used to send the data to the external script
The Python script executes as a reduce job as STREAM is called after GROUP BY
The result is stored in the relation clustered_jt
*/
jt_trimmed_grpd = GROUP jt_trimmed BY job_title;
jt_flattened = FOREACH jt_trimmed_grpd GENERATE flatten(group);
clustered_jt = STREAM jt_flattened THROUGH cluster_contacts;

/*
Clustered job titles from relation clustered_jt are typecasted to chararray and are assigned to relation clustered_jt_cast.
clustered_jt_cast relation contains job title clusters.  
*/
clustered_jt_cast = FOREACH clustered_jt GENERATE (chararray)$0 AS cluster;

/*
These job titles are tokenized by using comma and are assigned to the relation clustered_jt_tokens along with the cluster name.
*/
clustered_jt_tokens  = FOREACH clustered_jt_cast GENERATE TOKENIZE(cluster,','), cluster;

/*
Each job title in job cluster is converted into a new tuple and is assigned to relation clustered_jt_flattened along with the cluster name.
*/
clustered_jt_flattened = FOREACH clustered_jt_tokens  GENERATE FLATTEN($0) AS cluster_job, cluster;

/*
Trim spaces in the job titles.
*/
clustered_jt_trimmed  = FOREACH clustered_jt_flattened GENERATE TRIM(cluster_job) AS cluster_job, cluster;

/*
Join jt_trimmed relation by job_title with the relation clustered_jt_trimmed by cluster_job. Project the contact name and cluster name.
*/
jt_clustered_joind = JOIN jt_trimmed BY job_title,clustered_jt_trimmed  BY cluster_job;
name_clustered_jt = FOREACH jt_clustered_joind GENERATE jt_trimmed::name AS name, clustered_jt_trimmed::cluster AS cluster;

/*
Remove duplicate tuples from relation name_clustered_jt.
*/
uniq_name_clustered_jt  = DISTINCT name_clustered_jt;

/*
Group the relation uniq_name_clustered_jt by field cluster and project the clustername(consisting of a set of job titles) and the contact name
*/
name_clustered_jt_grpd =  GROUP uniq_name_clustered_jt  BY cluster;
similar_jt_clusters= FOREACH name_clustered_jt_grpd GENERATE group AS clustername, uniq_name_clustered_jt.name AS name;

/*
The results are stored on the HDFS in the directory clustering
*/
STORE similar_jt_clusters into '/user/cloudera/pdp/output/advanced_patterns/clustering';
