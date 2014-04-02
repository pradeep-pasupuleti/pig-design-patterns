package com.xmlgenerator;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

/*
 * customized record writer for writing xml document.
 */
public class XMLRecordWriter extends RecordWriter<NullWritable, Tuple>{
	
	private XMLElementWriter writer;
	/**
	 * Instantiating xmlelementwriter
	 * @param writer
	 */
	public XMLRecordWriter(XMLElementWriter writer)
	{
		this.writer = writer;
	}
	// Transforming XML domSource to a streamResult
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		writer.close(context);		
	}

	/*
	 * Appending xml element to document
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(NullWritable arg0, Tuple tuple) throws IOException,
			InterruptedException {
		writer.write(tuple);		
	}
	
}