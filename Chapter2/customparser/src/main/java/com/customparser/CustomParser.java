package com.customparser;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.customparser.avro.HealthTopicsHealthTopic;
import com.customparser.avro.HealthTopicsHealthTopicGroup;
import com.customparser.avro.HealthTopicsHealthTopicLanguageMappedTopic;
import com.customparser.avro.HealthTopicsHealthTopicMeshHeading;
import com.customparser.avro.HealthTopicsHealthTopicMeshHeadingDescriptor;
import com.customparser.avro.HealthTopicsHealthTopicMeshHeadingQualifier;
import com.customparser.avro.HealthTopicsHealthTopicOtherLanguage;
import com.customparser.avro.HealthTopicsHealthTopicPrimaryInstitute;
import com.customparser.avro.HealthTopicsHealthTopicRelatedTopic;
import com.customparser.jaxb.HealthTopics;
import com.customparser.jaxb.HealthTopics.HealthTopic.Group;
import com.customparser.jaxb.HealthTopics.HealthTopic.LanguageMappedTopic;
import com.customparser.jaxb.HealthTopics.HealthTopic.MeshHeading;
import com.customparser.jaxb.HealthTopics.HealthTopic.OtherLanguage;
import com.customparser.jaxb.HealthTopics.HealthTopic.PrimaryInstitute;
import com.customparser.jaxb.HealthTopics.HealthTopic.RelatedTopic;
import com.customparser.jaxb.HealthTopics.HealthTopic.MeshHeading.Descriptor;
import com.customparser.jaxb.HealthTopics.HealthTopic.MeshHeading.Qualifier;


public class CustomParser {

	public static void main(String args[])
	{
		
		try 
		{
			JAXBContext jaxbContext = JAXBContext.newInstance(com.customparser.jaxb.HealthTopics.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller(); 
			// specify the location and name of xml file to be read  
			
			File XMLfile = new File(args[1]);
			HealthTopics jaxbHTopics=(HealthTopics) jaxbUnmarshaller.unmarshal(XMLfile);
			List<HealthTopics.HealthTopic> jaxbHTopicsList = jaxbHTopics.getHealthTopic();
			Iterator<HealthTopics.HealthTopic> jaxbHTopicIterator=jaxbHTopicsList.iterator();
			
			File file = new File(args[0]);
			
			com.customparser.avro.HealthTopics hTopics = new com.customparser.avro.HealthTopics();
			DatumWriter<com.customparser.avro.HealthTopics> datumFileWriter = new SpecificDatumWriter<com.customparser.avro.HealthTopics>();
			DataFileWriter <com.customparser.avro.HealthTopics> dataFileWriter = new DataFileWriter<com.customparser.avro.HealthTopics>(datumFileWriter);
			
			dataFileWriter.create(hTopics.getSchema(), file);
			
			List<HealthTopicsHealthTopic> hTopicList = new ArrayList<HealthTopicsHealthTopic>();
			
			while(jaxbHTopicIterator.hasNext())
			{
				HealthTopics.HealthTopic jaxbHTopic = jaxbHTopicIterator.next();
				
				HealthTopicsHealthTopic hTopic = new HealthTopicsHealthTopic();
				hTopic.put(0, jaxbHTopic.getAlsoCalled());
				hTopic.put(1, jaxbHTopic.getFullSummary());
				
				List<Group> jaxbGroupList=jaxbHTopic.getGroup();
				Iterator<Group> groupIterator = jaxbGroupList.iterator();
				List<HealthTopicsHealthTopicGroup> groupList= new ArrayList<HealthTopicsHealthTopicGroup>();
				while(groupIterator.hasNext())
				{
					HealthTopicsHealthTopicGroup group = new HealthTopicsHealthTopicGroup();
					Group jaxbGroup= groupIterator.next();
							
					group.put(0, jaxbGroup.getValue());
					group.put(1, jaxbGroup.getUrl());
					//group.put(2, gp.getId());
					groupList.add(group);
				}
				
				hTopic.put(2, groupList);
				
				LanguageMappedTopic jaxbLMT=jaxbHTopic.getLanguageMappedTopic();
				HealthTopicsHealthTopicLanguageMappedTopic langmappedTopic= new  HealthTopicsHealthTopicLanguageMappedTopic();
				
				if(jaxbLMT!=null)
				{
				langmappedTopic.put(0, jaxbLMT.getValue());
				langmappedTopic.put(1, jaxbLMT.getUrl());
				langmappedTopic.put(2, jaxbLMT.getId());
				langmappedTopic.put(3, jaxbLMT.getLanguage());
				
				hTopic.put(3, langmappedTopic);
				}
				List<MeshHeading> jaxbMeshHeadingList=jaxbHTopic.getMeshHeading();
				Iterator<MeshHeading> meshHeadingIterator = jaxbMeshHeadingList.iterator();
				List<HealthTopicsHealthTopicMeshHeading> meshHeadingList= new ArrayList<HealthTopicsHealthTopicMeshHeading>();
				while(meshHeadingIterator.hasNext())
				{
					HealthTopicsHealthTopicMeshHeading meshHeading = new HealthTopicsHealthTopicMeshHeading();
					MeshHeading jaxbMeshHeading= meshHeadingIterator.next();
					
					HealthTopicsHealthTopicMeshHeadingDescriptor descriptor = new HealthTopicsHealthTopicMeshHeadingDescriptor();
					HealthTopicsHealthTopicMeshHeadingQualifier qualifier = new HealthTopicsHealthTopicMeshHeadingQualifier();
					
					Descriptor jaxbDescriptor = jaxbMeshHeading.getDescriptor();
					Qualifier jaxbQulifier = jaxbMeshHeading.getQualifier();
					
					descriptor.put(0, jaxbDescriptor.getValue());
					descriptor.put(1, jaxbDescriptor.getId());
					if(jaxbQulifier!=null)
					{
					qualifier.put(0, jaxbQulifier.getValue());
					qualifier.put(1, jaxbQulifier.getId());
					meshHeading.put(1, qualifier);
					
					}
					meshHeading.put(0, descriptor);

					meshHeadingList.add(meshHeading);
				}
				
				hTopic.put(4, meshHeadingList);
				
				List<OtherLanguage> jaxbOtherLangList=jaxbHTopic.getOtherLanguage();
				Iterator<OtherLanguage> otherLangIterator = jaxbOtherLangList.iterator();
				List<HealthTopicsHealthTopicOtherLanguage> otherLangList= new ArrayList<HealthTopicsHealthTopicOtherLanguage>();
				while(otherLangIterator.hasNext())
				{
					HealthTopicsHealthTopicOtherLanguage otherLang = new HealthTopicsHealthTopicOtherLanguage();
					OtherLanguage jaxbOtherLang= otherLangIterator.next();
					
					otherLang.put(0, jaxbOtherLang.getValue());
					otherLang.put(1, jaxbOtherLang.getVernacularName());
					otherLang.put(2, jaxbOtherLang.getUrl());
					
					otherLangList.add(otherLang);
				}
				hTopic.put(5, otherLangList);
				
				PrimaryInstitute jaxbPrimaryInstitute=jaxbHTopic.getPrimaryInstitute();
				HealthTopicsHealthTopicPrimaryInstitute primaryInstitute= new  HealthTopicsHealthTopicPrimaryInstitute();
				
				if(jaxbPrimaryInstitute!=null)
				{
				primaryInstitute.put(0, jaxbPrimaryInstitute.getValue());
				primaryInstitute.put(1, jaxbPrimaryInstitute.getUrl());

				hTopic.put(6, primaryInstitute);
				}
				List<RelatedTopic> jaxbRelatedtopic=jaxbHTopic.getRelatedTopic();
				Iterator<RelatedTopic> jaxbRelatedTopicIterator = jaxbRelatedtopic.iterator();
				List<HealthTopicsHealthTopicRelatedTopic> relatedTopicList= new ArrayList< HealthTopicsHealthTopicRelatedTopic>();
				
				while(jaxbRelatedTopicIterator.hasNext())
				{
					HealthTopicsHealthTopicRelatedTopic relatedTopic = new HealthTopicsHealthTopicRelatedTopic();
					RelatedTopic jaxbRelatedTopic = jaxbRelatedTopicIterator.next();
					
					relatedTopic.put(0, jaxbRelatedTopic.getValue());
					relatedTopic.put(1, jaxbRelatedTopic.getUrl());
					relatedTopic.put(2, jaxbRelatedTopic.getId());
					relatedTopicList.add(relatedTopic);
				}
				hTopic.put(7, relatedTopicList);

				hTopic.put(8, jaxbHTopic.getSeeReference());
				hTopic.put(9, jaxbHTopic.getTitle());
				hTopic.put(10, jaxbHTopic.getUrl());
				hTopic.put(11, jaxbHTopic.getId());
				hTopic.put(12, jaxbHTopic.getLanguage());
				hTopic.put(13, jaxbHTopic.getDateCreated());
				
				hTopicList.add(hTopic);
			}
			
			hTopics.put(0, hTopicList);			
			hTopics.put(1, jaxbHTopics.getTotal());
			hTopics.put(2, jaxbHTopics.getDateGenerated());
			
			dataFileWriter.append(hTopics);
			dataFileWriter.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}