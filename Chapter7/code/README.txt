Below are the prerequisites for executing cluster_contacts.pig and named_entities.pig (these Pig scripts call Python scripts via streaming)
Python should be installed on all the nodes of the Hadoop cluster.
The dependent modules such as nltk should be available on all the nodes and their path specified in the environment variable PYTHON_PATH
The Python scripts should have execute permissions on them.