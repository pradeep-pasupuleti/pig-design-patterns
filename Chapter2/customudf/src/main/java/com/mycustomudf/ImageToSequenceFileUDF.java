package com.mycustomudf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ImageToSequenceFileUDF extends EvalFunc<Tuple> {

	protected TupleFactory tupleFactory = TupleFactory.getInstance();
	static FileSystem fileSystem;
	static Configuration confHadoop;
	@Override
	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			return tupleFactory.newTuple("NULL");
		}

		confHadoop = new Configuration();
		fileSystem = FileSystem.get(confHadoop);
		String inputPath = input.get(0).toString();
		Path inPutPath = new Path(inputPath);
		// invoking createSequenceFile method, which creates sequence file for the images available in the input path
		String seqFilePath = createSequenceFile(inPutPath);
		//returning sequence file name by appending to tuple 
		return tupleFactory.newTuple(seqFilePath);
	}
	/**
	 * The method reads images available in the input path and generates a sequence file for these images
	 * @param inPutPath images input path
	 * @return sequence file path 
	 */
	public static String createSequenceFile(Path inPutPath)
	{
		String	outputPath="";
		FSDataInputStream dataInputStream = null;
		SequenceFile.Writer seqFileWriter = null;
		
		try {
			/*
			 * sequence file path
			 */
			outputPath = inPutPath+"images.seq";
			
			Path outPath = new Path(outputPath);
			String fileName = "";
			/*
			 * sequence file writer instance
			 */
			seqFileWriter = SequenceFile.createWriter(fileSystem, confHadoop, outPath,
					Text.class, BytesWritable.class);
			
			
			FileStatus filestatus = fileSystem.getFileStatus(inPutPath);

			if (filestatus.isDir()) {

				
				FileStatus[] status = fileSystem.listStatus(inPutPath);

				for(int i=0;i<status.length;i++)
				{
					//FSDataInputStream is opened at the given path
					dataInputStream = fileSystem.open(status[i].getPath());
					
					// extracting image name from the absolute path
					fileName = status[i].getPath().toString().substring(status[i].getPath().toString().lastIndexOf("/")+1);
					
					byte buffer[] = new byte[dataInputStream.available()];
					
					//buffer.remaining() bytes will be read into buffer.
					dataInputStream.read(buffer);
					
					/*Add a key/value pair. Key is the image filename and 
					value is the BytesWritable object*/

					seqFileWriter.append(new Text(fileName), new BytesWritable(buffer));
				} 
			}
			seqFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(seqFileWriter);
		}
		return outputPath;
	}
}