package com.profiler;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class DataTypeInfererUDF extends EvalFunc<String> {
	private static String LONG = "long";
	private static String INT = "int";
	private static String DOUBLE = "double";
	private static String BOOLEAN_TRUE = "true";
	private static String BOOLEAN_FALSE = "false";
	private static String BOOLEAN = "boolean";
	private static String STRING = "chararray";
	private static String NULL = "nulls";

	public DataTypeInfererUDF() {
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public String exec(Tuple tuples) throws IOException {

		String value = (String) tuples.get(0);
		String inferredType = null;
		try {
			// if tuples.get(0) is null it returns null else invokes getDataType() method to infer the datatype
			inferredType = value != null ? getDataType(value) : NULL;

		} catch (Exception e) {
			e.printStackTrace();
		}
		// returns inferred datatype of the input value
		return inferredType;
	}
	/**
	 * Checks for Double. If input is of type Double, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isDouble(String str) {
		try {
			Double.parseDouble(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for Boolean. If input is of type Boolean, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isBoolean(String str) {
		try {
			if (str.trim().equalsIgnoreCase(BOOLEAN_TRUE)
					|| str.trim().equalsIgnoreCase(BOOLEAN_FALSE))
				return true;
			else
				return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for Long. If input is of type Long, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isLong(String str) {
		try {
			Long.parseLong(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for Integer. If input is Int then returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for null. If input is null, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isNull(Object str) {
		try {
			if (str != null && str.toString() != null
					&& str.toString().trim().length() > 0)
				return false;
			else
				return true;
		} catch (Exception e) {
			return true;
		}
	}
	/**
	 * Gets the data type of the given input
	 * @param value
	 * @return input value data type
	 */
	public String getDataType(String value) {

		if (isNull(value)) // checks for null
			return NULL;
		else if (isInt(value.toString())) // checks for Integer
			return INT;
		else if (isLong(value.toString())) // checks for long
			return LONG;
		else if (isDouble(value.toString())) // checks for double
			return DOUBLE;
		else if (isBoolean(value.toString())) // checks for boolean
			return BOOLEAN;
		return STRING;
	}
}