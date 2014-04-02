package com.validation;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class DataTypeValidationUDF extends EvalFunc<String>{

	private String dataType;

	private static String BOOLEAN_TRUE = "true";
	private static String BOOLEAN_FALSE = "false";

	/**
	  * Constructor. 
	  * @param dataType (required) Data type used to validate the values
	  *
	  */
	public DataTypeValidationUDF(String dataType)
	{
		this.dataType =  dataType;
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public String exec(Tuple tuples) throws IOException {
		String value = (String) tuples.get(0);
		String invalidValue = null;
		try 
			{
			/*The value is assigned to invalidValue if it is not null and it does not match the data type passed to the constructor 
			In all other cases it returns null
			*/
				if(value != null && !isValidDataType(value))
				{
					invalidValue = value;
				}

			} catch (Exception e) {
				e.printStackTrace();
			} 
		return invalidValue;
	}
	
	public boolean isValidDataType(String value)
	{
		/* 
		 * Checks for Int type if the dataType is passed as int
		 */
		if(dataType.equalsIgnoreCase("int"))
			return isInt(value.toString());
		/* 
		 * Checks for Long type if the dataType is passed as long
		 */
		else if(dataType.equalsIgnoreCase("long"))
			return isLong(value.toString());
		/* 
		 * Checks for Float type if the dataType is passed as float
		 */
		else if(dataType.equalsIgnoreCase("float"))
			return isFloat(value.toString());
		/* 
		 * Checks for Double type if the dataType is passed as double
		 */
		else if(dataType.equalsIgnoreCase("double"))
			return isDouble(value.toString());
		/* 
		 * Checks for Boolean type if the dataType is passed as boolean
		 */
		else if(dataType.equalsIgnoreCase("boolean"))
			return isBoolean(value.toString());
		/* 
		 * Checks for String type if the dataType is passed as chararray
		 */
		else if(dataType.equalsIgnoreCase("chararray"))
			return isString(value.toString());
		return false;
	}

	/**
	 * Checks for Boolean. If input is of type Boolean, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isBoolean(String str) 
	{
		try 
		{
			if(str.trim().equalsIgnoreCase(BOOLEAN_TRUE) || str.trim().equalsIgnoreCase(BOOLEAN_FALSE))
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
	boolean isLong(String str) 
	{
		try 
		{
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
	boolean isInt(String str) 
	{
		try 
		{
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for Double. If input is of type Double, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isDouble(String str) 
	{
		try 
		{
			Double.parseDouble(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for Float. If input is of type Float, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isFloat(String str) 
	{
		try 
		{
			Float.parseFloat(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	 * Checks for String. If input is of type String, it returns true else returns false
	 * @param str
	 * @return
	 */
	boolean isString(String str)
	{
		return str.matches(".*[a-zA-Z]+.*");
	}
}