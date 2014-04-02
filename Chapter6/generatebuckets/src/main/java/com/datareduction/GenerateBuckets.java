package com.datareduction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class GenerateBuckets extends EvalFunc<String> {

	private double max;
	private long bucketSize;
	private long noOfBuckets;
	List<Long> bucketRange = null;

	public GenerateBuckets(String noOfBins) {
		this.noOfBuckets = Long.parseLong(noOfBins);
	}
	/*
	Calculate the bucket size by dividing maximum transaction amount by the number of buckets.
	*/
	public void setBucketSize()
	{
		bucketSize=0;
		bucketRange = new ArrayList<Long>();
		bucketSize =(long) Math.ceil((max)/noOfBuckets);
		bucketSize = ((bucketSize/10)+1)*10;
		System.out.println("bucketSize="+bucketSize);
	}

	private void setBucketRange()
	{
		for(int counter=1;counter<=noOfBuckets;counter++)
		{
			bucketRange.add(bucketSize*counter);
		}
		System.out.println("noOfBuckets"+noOfBuckets);
		System.out.println("bucketRange.size() ="+bucketRange.size());
	}
	//Compute the range to which each value belongs to and return the value along with the bucket range.
	private String getBucketRange(double rangeval)
	{
		int count=0;
		for(int iterator=0;iterator<noOfBuckets;iterator++)
		{
			if( rangeval < bucketRange.get(iterator))
				return iterator==0? "1-"+bucketRange.get(iterator):bucketRange.get(iterator-1)+"-"+bucketRange.get(iterator);
				count++;
		}
		if(count>1)
			return bucketRange.get(count-2)+"-"+bucketRange.get(count-1);
		else
			return"1-"+bucketRange.get(count-1);
	}
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() ==0)
			return null;
		try{
			//Extract the maximum transaction amount
			max = Double.parseDouble(input.get(0).toString());
			//Extract the value
			double rangeval = Double.parseDouble(input.get(1).toString());
			/*
			Calculate the bucket size by dividing maximum transaction amount by the number of buckets.
			*/
			setBucketSize();
			
			/*
			Set the bucket range by using the bucketSize and noOfBuckets
			*/
			setBucketRange();
			
			/*
			It finds out the range to which each value belongs to and returns the value along with the bucket range
			*/
			return getBucketRange(rangeval);
		} catch(Exception e){
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}

	}
	public Schema outputSchema(Schema input) {
		try{
			Schema bagSchema = new Schema();
			bagSchema.add(new Schema.FieldSchema("range", DataType.CHARARRAY));
			return bagSchema;
		}catch (Exception e){
			return null;
		}
	}
}