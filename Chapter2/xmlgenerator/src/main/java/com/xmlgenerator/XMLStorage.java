package com.xmlgenerator;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

public class XMLStorage extends StoreFunc implements StoreMetadata {

	protected RecordWriter writer;
	protected ResourceSchema schema = null;
	
	private String udfcSignature = null;
	private static final String SCHEMA_SIGNATURE = "pig.xmlstorage.schema";
	public static String LOCATION = "location";
	public static String rootElementName = "rootelement";
	public static String elementName = "rowelement";
	private static final int BUF_SIZE = 4 * 1024; 
	
	
	/*public XMLStorage(String rootElementName,String elementName)
	{
		this.rootElementName = rootElementName;
		this.elementName = elementName;
	}*/
	
	public XMLStorage()
	{
		this.rootElementName = "transactions";
		this.elementName = "transaction";
	}
	
	@Override
	public void storeSchema(ResourceSchema arg0, String arg1, Job arg2)
			throws IOException {
		
	}

	@Override
	public void storeStatistics(ResourceStatistics arg0, String arg1, Job arg2)
			throws IOException {
		// TODO Auto-generated method stub
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#getOutputFormat()
	 */
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		
		/*
		 * gets a reference to UDFContext
		 * UDFContext allows data from frontend to pass the backend
		 */
		UDFContext udfc = UDFContext.getUDFContext();
		//Gets reference to properties object kept by UDFContext
		Properties p =udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		
		//return property value of the specified key
		String strSchema = p.getProperty(SCHEMA_SIGNATURE);
		
		if (strSchema == null)
		{
			throw new IOException("Could not find schema in UDF context");
		}
		
		//schema representation for store function communication
		schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
		
		XMLOutputFormat xmlOutputFormat = new XMLOutputFormat(schema);
		return xmlOutputFormat;
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
	 */
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		
		this.writer = writer;
		
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p =udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		String strSchema = p.getProperty(SCHEMA_SIGNATURE);
		
		if (strSchema == null)
		{
			throw new IOException("Could not find schema in UDF context");
		}
		
		schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
		
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#putNext(org.apache.pig.data.Tuple)
	 */
	@Override
	public void putNext(Tuple tuple) throws IOException {
		try {
			this.writer.write(NullWritable.get(), tuple);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
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
		
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		LOCATION = location;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#setStoreFuncUDFContextSignature(java.lang.String)
	 */
	public void setStoreFuncUDFContextSignature(String signature){
		udfcSignature = signature;
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.StoreFunc#checkSchema(org.apache.pig.ResourceSchema)
	 */
	public void checkSchema(ResourceSchema s) throws IOException {
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		 p.setProperty(SCHEMA_SIGNATURE, s.toString());
	}
}