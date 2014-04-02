package com.datareduction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

/**
 * This class converts Vectors written in SequenceFiles by Pig script using elephant-bird to
 * NamedVectors Input directory path is theoutput directory path is the second argument
 */
public class VectorConverter {
    static Logger LOGGER = Logger.getLogger(VectorConverter.class);
    static String usage = "Usage: hadoop jar <jarfilename> <classname> <input directory> <outputdirectory> ";
 
    public static void main(String[] args) {
		/*Check the number of arguments passed, log error message if number o arguments is not equal to 2*/
    	if (args.length != 2) {
            LOGGER.error(usage);
            System.exit(-1);
        }

        String inPath = args[0];
        String outPath = args[1];
        LOGGER.info(String.format("inputdir='%s', outputdir='%s'", inPath, outPath));
        try {
            // Open the sequence file from the input directory path
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            FileStatus fileStatus = fs.getFileStatus(inputPath);
            Path outputPath = new Path(outPath);
            // Check if input and output arguments are directories
            if (!fileStatus.isDir()) {
                LOGGER.error(String.format("'%s' is not a directory.\n%s", inPath,
                        usage));
                System.exit(-1);
            }

            fileStatus = fs.getFileStatus(outputPath);
            if (!fileStatus.isDir()) {
                LOGGER.error(String.format("'%s' is not a directory.\n%s", outPath,
                        usage));
                System.exit(-1);
            }

            // Iterate over SequenceFile entries and output NamedVectors
            Text key = new Text();
            VectorWritable value = new VectorWritable();
            long id = 1;
            long cnt = 0;
            int fn_count = 0;
            Path outfile = new Path(outputPath, String.format("part-m-%05d",
                    fn_count++));
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                    outfile, Text.class, VectorWritable.class);
            for (Pair<Text, VectorWritable> entry : new SequenceFileDirIterable<Text, VectorWritable>(
                    new Path(inputPath, "part-*"), PathType.GLOB, conf)) {
                key = entry.getFirst();
                value = entry.getSecond();
                NamedVector vec = new NamedVector(value.get(), key.toString());
                value.set(vec);
                writer.append(new Text(Long.toString(id++)), value);
                cnt++;
                if (cnt >= 817740) {
                    LOGGER.info(String.format(
                            "Wrote '%d' namedvectors to '%s'", cnt,
                            outfile.toString()));
                    writer.close();
                    cnt = 0;
                    outfile = new Path(outputPath, String.format("part-m-%05d",
                            fn_count++));
                }
            }
            // write remaining entries
            if (cnt > 0) {
                writer.close();
                LOGGER.info(String.format("Wrote '%d' namedvectors to '%s'",
                        cnt, outfile.toString()));
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}