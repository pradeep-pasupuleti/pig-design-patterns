/*
Register the custom loader imagelibrary.jar, it has UDFs to convert images to sequence file and sequence file to images
*/
REGISTER '/home/cloudera/pdp/jars/imagelibrary.jar';

/*
Load images_input file, it contains the path to images directory
*/
images_file_path = LOAD '/user/cloudera/pdp/datasets/images/images_input' AS (link:chararray);

/*
ImageToSequenceFileUDF converts multiple image files to a sequence file. 
This ensures that there are no large number of small files on HDFS, instead multiple small images are converted into a single sequence file.
Another advantage of sequence file is that it is splittable.
The sequence file contains key value pairs, key will be the image file name and value is the image binary data.
It returns the path of the sequence file.
*/
convert_to_seq = FOREACH images_file_path GENERATE com.mycustomudf.ImageToSequenceFileUDF(); 

/*
* Some processing logic goes here which is deliberately left out to improve readability
*/

-- Display the contents of the convert_to_seq on the console
DUMP convert_to_seq;
