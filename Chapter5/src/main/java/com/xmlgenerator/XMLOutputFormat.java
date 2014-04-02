package com.xmlgenerator;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;

/*
 * Customized OutputFormat,OutputFormat specifies the output description for  Mapper and Reducer job.
 */
public class XMLOutputFormat extends FileOutputFormat<NullWritable, Tuple> {

	public final static String EXTENSION = ".xml";
    private ResourceSchema schema =null;
    
    public XMLOutputFormat(){  	
    }
    
    
    public XMLOutputFormat(ResourceSchema schema ){  	
    	this.schema = schema;
    }

	/*
     * Customized record writer, which returns XMLRecordWriter object.
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */  
    @Override
	public RecordWriter<NullWritable, Tuple> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		if (schema == null)
            throw new IOException("Must provide a schema");
		
		Path path = getDefaultWorkFile(context, EXTENSION);
		XMLElementWriter writer = new XMLElementWriter(schema,path);		
		return new XMLRecordWriter(writer);
	}    
}