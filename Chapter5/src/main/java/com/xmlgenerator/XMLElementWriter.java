package com.xmlgenerator;

import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XMLElementWriter {

	protected ResourceSchema schema = null;
	Document xmlDoc = null;
	Element rootElement = null;
	FSDataOutputStream outputStram =null;
	Path path=null;
	DocumentBuilderFactory documentBuilderFactory = null;
	DocumentBuilder documentBuilder = null;
	/**
	 * Parameterized constructor, which takes schema and path input parameters
	 * @param schema
	 * @param path
	 */
	public XMLElementWriter(ResourceSchema schema, Path path)
	{
		this.schema = schema ;
		this.path = path ;
		//Gets DocumentBuilderFactory instance 
		documentBuilderFactory = DocumentBuilderFactory.newInstance();
		try {
			/*
			 * Creates DocumentBuilder instance.
			 * After this class instance is obtained, XML can be parsed from multiple input sources. 
			 * The input sources can be InputStreams, URLs,Files, and SAX InputSources.
			 */
			documentBuilder = documentBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		/*
		 * Obtains DOM Document object instance to form DOM tree.The Document interface represents the entire HTML or XML document. 
		 * Document object represents document tree root, and obtains access to the document's data.
		 */
		xmlDoc = documentBuilder.newDocument();
		/*
		 * creating xml root element
		 */
		rootElement = xmlDoc.createElement(TransformStoreXML.rootElementName);
	}
	/**
	 * data from tuple is appended to xml root element
	 * @param tuple
	 */
	protected void write(Tuple tuple)
	{
		// Retrieving all fields from the schema
		ResourceFieldSchema[] fields = schema.getFields();
		//Retrieve values from tuple
		List<Object> values = tuple.getAll();
		//Creating xml element by using fields as element tag and tuple value as element value
		Element transactionElement = xmlDoc.createElement(TransformStoreXML.elementName);
		for(int counter=0;counter<fields.length;counter++)
		{
			//Retrieving element value from values
			String columnValue =  String.valueOf(values.get(counter));
			//Creating element tag from fields
			Element columnName =  xmlDoc.createElement(fields[counter].getName().toString().trim());
			//Appending value to element tag
			columnName.appendChild(xmlDoc.createTextNode(columnValue));
			//Appending element to transaction element
            transactionElement.appendChild(columnName);		
		}
		//Appending transaction element to root element
		rootElement.appendChild(transactionElement);
	}
	
	
	protected void close(TaskAttemptContext context)
	{
		//Appending root element to the xml document
		xmlDoc.appendChild(rootElement);
		//Creation of TransformerFactory instance,which is used for creation of  Transformer objects
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		try {
			//creation of Transformer instance,which can be used to process XML from I/P source and writes to the O/P.
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		}
		//reference to the JobConf.
		Configuration conf = UDFContext.getUDFContext().getJobConf();
		try {
			//Return the FileSystem that owns this Path. Create an FSDataOutputStream at the indicated Path.
			//Reference to the FileSystem owing the path, From FileSystem reference,  FSDataOutputStream instance is created
			outputStram = path.getFileSystem(conf).create(new Path(TransformStoreXML.LOCATION+path.SEPARATOR+path.getName()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//DOMSource instance creation
		DOMSource source = new DOMSource(xmlDoc);
		//StreamResult is constructed from byte stream.
		StreamResult result = new StreamResult(outputStram);
		try {
			//Transforming XML domSource to a streamResult.
			transformer.transform(source, result);
		} catch (TransformerException e) {
			e.printStackTrace();
		}

	}
}