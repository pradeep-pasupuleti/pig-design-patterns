package com.profiler.unstructuredtextprofiling;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GenerateTFIDF extends EvalFunc<Double>{
	@Override
	/**
	*The pre-calculated wordCount, wordCountPerDoc, totalDocs and docCountPerWord are passed as parameters to this UDF.
	*/
	public Double exec(Tuple input) throws IOException {
		/*
		Retrieve the values from the input tuple
		*/
		long countOfWords = (Long) input.get(0);
		long countOfWordsPerDoc = (Long) input.get(1);
		long noOfDocs = (Long) input.get(2);
		long docCountPerWord = (Long) input.get(3);
		/*
		Compute the overall relevancy of a document with respect to a term. 
		*/
		double tf = (countOfWords * 1.0) / countOfWordsPerDoc;
		double idf = Math.log((noOfDocs * 1.0) / docCountPerWord);
		return tf * idf;
	}
}
