package com.datareduction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

public class CsvToSequenceConverter {

	public CsvToSequenceConverter() {
	}

	public static final int NUM_COLUMNS = 13;

	public static void main(String[] args) {
		//Input path on HDFS
		String INPUT_FILE = args[0];
		//Output path on HDFS
		String OUTPUT_FILE = args[1];
		
		List<NamedVector> namedVectorList = new ArrayList<NamedVector>();
		NamedVector namedVector;
		BufferedReader br = null;
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = null;
		
		try {
			//Create FileSystem object by passing configuration object
			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path(INPUT_FILE);
			Path outputPath = new Path(OUTPUT_FILE);
			//Read lines from the input file
			br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				/*
				 * Split the line with comma to get the values. Assign the first value as key
				*/
				String itemName = currentLine.split(",")[0];
				double[] features = new double[NUM_COLUMNS - 1];
				for (int indx = 1; indx < NUM_COLUMNS; ++indx) {
					features[indx - 1] = Float.parseFloat(currentLine
							.split(",")[indx]);
				}
				namedVector = new NamedVector(new DenseVector(features),
						itemName);
				namedVectorList.add(namedVector);
			}
			//Create sequence file with Text as Key and VectorWritable as value
			writer = new SequenceFile.Writer(fs, conf, outputPath, Text.class,
					VectorWritable.class);
			VectorWritable vec = new VectorWritable();
			for (NamedVector vector : namedVectorList) {
				vec.set(vector);
				writer.append(new Text(vector.getName()), vec);
			}
		} catch (IOException ie) {
			ie.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("written to file");
	}
}