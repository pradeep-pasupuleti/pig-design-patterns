package com.profiler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;

public class CustomDatatypeInfererStorage extends StoreFunc implements StoreMetadata {

	protected RecordWriter<Text, Text> writer = null;
	protected ResourceSchema schema = null;
	String columnName = "";
	String definedDatatype = "";
	public CustomDatatypeInfererStorage(String columnName,String definedDatatype)
	{
		this.columnName = columnName;
		this.definedDatatype = definedDatatype;
	}
	
	@Override
	public void storeSchema(ResourceSchema arg0, String arg1, Job arg2)
			throws IOException {
	}

	@Override
	public void storeStatistics(ResourceStatistics arg0, String arg1, Job arg2)
			throws IOException {

	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#getOutputFormat()
	 */
	@Override
	public OutputFormat<Text, Text> getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, Text>();
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
	 */
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
		try {
			// Writing column name to destination path
			this.writer.write(new Text("Column Name :"), new Text(columnName));
			//Writing Defined datatype to destination path
			this.writer.write(new Text("Defined Datatype :"), new Text(definedDatatype));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#putNext(org.apache.pig.data.Tuple)
	 */
	@Override
	public void putNext(Tuple tuples) throws IOException {

		List<Object> tuple = tuples.getAll();
		
		try {
			// Writing Null count to destinaltion path 
			if(tuple.get(0).toString().equalsIgnoreCase("nulls"))
			{
				writer.write(new Text("Null Count:"), new Text(tuple.get(1).toString()+" Percentage: "+tuple.get(2).toString()));
			}
			//Writing Dominent data type to destinaltion path Dominant
			else if(tuple.get(3).toString().equalsIgnoreCase("Dominant"))
			{
				writer.write(new Text("Inferred Dominant Datatype(s):"), new Text(tuple.get(0).toString()+", Count: "+tuple.get(1).toString()+" Percentage: "+tuple.get(2).toString()));
			}
			//Writing Other data type to destinaltion path 
			else if(tuple.get(3).toString().equalsIgnoreCase("Other"))
			{
				writer.write(new Text("Inferred Other Datatype(s):"), new Text(tuple.get(0).toString()+", Count: "+tuple.get(1).toString()+" Percentage: "+tuple.get(2).toString()));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#setStoreLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
	 */
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}
}