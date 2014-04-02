
package com.mycustomudf;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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

public class SequenceToImageStorage extends StoreFunc implements StoreMetadata {

	protected RecordWriter<NullWritable, Tuple> writer = null;
	protected ResourceSchema schema = null;
	private String location="";
	@Override
	public void storeSchema(ResourceSchema arg0, String arg1, Job arg2)
	throws IOException {
	}

	@Override
	public void storeStatistics(ResourceStatistics arg0, String arg1, Job arg2)
	throws IOException {

	}
	@Override
	public OutputFormat<Text, NullWritable> getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, NullWritable>();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple tuples) {

		List<Object> tuple = tuples.getAll();
		Path outPutPath=null;
		Path inPutPath=null;
		String inputPath = tuple.get(0).toString();
		if(inputPath.contains("(") || inputPath.contains(")"))
			inputPath = inputPath.substring(inputPath.indexOf("(")+1, inputPath.indexOf(")"));
		SequenceFile.Reader seqFilereader = null;
		FSDataOutputStream seqFileWriter = null;

		Configuration confHadoop = new Configuration();
		String defaultName = confHadoop.get("fs.default.name");
		WritableComparable key=null;
		Writable value = null;		
		FileSystem fileSystem = null;
		byte buffer[]=null;
		String bufferString[]=null;
		String seqFilePath = null;
		
		try{
		fileSystem = fileSystem.get(confHadoop);
		//relative sequence file path

		seqFilePath = fileSystem.getUri()+inputPath;
		
		inPutPath=new Path(seqFilePath);

		//Open the file.
		seqFilereader = new SequenceFile.Reader(fileSystem, inPutPath, confHadoop);


		//class of keys will be returned
		key = (WritableComparable) seqFilereader.getKeyClass().newInstance();

		// class of values will be returned
		value = (Writable) seqFilereader.getValueClass().newInstance();

		// Next key and value pair will be read into key and value
		while (seqFilereader.next(key, value))
		{
			bufferString = value.toString().split(" ");
			buffer =new byte[bufferString.length];
			for(int i=0;i<bufferString.length;i++)
			{
				//string parameter parsed as signed integer in the radix given by the second parameter

				buffer[i] = (byte) Integer.parseInt(bufferString[i], 16);
			}
			/* 
			output path of the image which is the path specified, key is the image name 
			 */

			outPutPath=new Path(location+"/"+key);

			// FSDataOutputStream will be created at the given Path.
			seqFileWriter = fileSystem.create(outPutPath);


			// All bytes in array are written to the output stream
			seqFileWriter.write(buffer);
			seqFileWriter.flush();
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try {
				seqFileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		//storage location
		this.location=location;
		FileOutputFormat.setOutputPath(job, new Path(location));
	}
}