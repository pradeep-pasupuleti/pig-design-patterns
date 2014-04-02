package com.profiler;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class CustomProfileStorage extends StoreFunc{
	
	 protected RecordWriter<Text, NullWritable> writer = null;
	 private static final String MODE ="Mode";
	 private static final String DIST_VALS="Distinct Values";
	 
	@Override
	public OutputFormat<Text, NullWritable> getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, NullWritable>();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple tuple) throws IOException {
		List<Object> values = tuple.getAll();
		
		Map<String,String> map =new LinkedHashMap<String, String>();
		
		for(int counter=0;counter<values.size();counter++)
		{
			if(values.get(counter)!=null && values.get(counter).toString().equalsIgnoreCase(MODE))
			{
				if( values.get(++counter) instanceof DataBag)
				{
					DataBag bag = (DataBag) values.get(counter);
					map.put(MODE, getMode(bag));
				}
				else
					map.put(MODE, values.get(counter).toString());
			}
			else if(values.get(counter)!=null && values.get(counter).toString().equalsIgnoreCase(DIST_VALS))
			{
				if( values.get(++counter) instanceof DataBag)
				{
					DataBag bag = (DataBag) values.get(counter);
					map.put(DIST_VALS, getValue(bag,map.get("Column Name")));
				}
			}
			else if(values.get(counter)!=null && values.get(counter+1)!=null)
				map.put(values.get(counter).toString(), values.get(++counter).toString());
		}
		
		try {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				if(entry.getValue().contains("$$$"))
				{
					String stringarr[] = entry.getValue().split("\\$\\$\\$");
					for(String str : stringarr)
					{
						writer.write(new Text(str.trim()),NullWritable.get());
					}
				}
				
				else
					writer.write(new Text(entry.getKey()+": "+entry.getValue()),NullWritable.get());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}
	public String getMode(DataBag bag)
	{
		String modeVals = "";
		 for (Iterator<Tuple> iterator =bag .iterator(); iterator.hasNext();) 
	        {
	            Tuple modeTuple = (Tuple)iterator.next();
	            try 
	            {
	            	List<Object> modeValues = modeTuple.getAll();
	            	modeVals += modeValues.get(1)+",";
	            }
	            catch(Exception e)
	            {
	            e.printStackTrace();	
	            }
	        }
		 if(modeVals!=null && modeVals.length() > 0 && modeVals.contains(","))
			 modeVals = modeVals.substring(0,modeVals.lastIndexOf(","));
		 return modeVals;
	
	}
	public String getValue(DataBag bag,String colName)
	{
		String modeVals = "Distinct Values $$$"+colName+"		Count		Percentage $$$";
		
		 for (Iterator<Tuple> iterator =bag .iterator(); iterator.hasNext();) 
	        {
	            Tuple modeTuple = (Tuple)iterator.next();
	            try 
	            {
	            	List<Object> modeValues = modeTuple.getAll();
	            	modeVals += modeValues.get(0)+"		"+modeValues.get(1)+"		"+modeValues.get(2)+"%$$$";
	            }
	            catch(Exception e)
	            {
	            e.printStackTrace();	
	            }
	        }
		 return modeVals;
	}
}