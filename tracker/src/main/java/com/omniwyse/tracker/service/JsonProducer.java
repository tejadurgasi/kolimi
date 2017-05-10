package com.omniwyse.tracker.service;

	import java.io.IOException;
import java.util.*;

	import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwyse.tracker.controller.Message;
	
	import kafka.javaapi.producer.Producer;
	import kafka.producer.KeyedMessage;
	import kafka.producer.ProducerConfig;
	
@Service
	public class JsonProducer {
	
		public void send(String str) throws JsonParseException, JsonMappingException, IOException {
	    
	    	Properties props = new Properties();
	    	props.put("metadata.broker.list", "kafka-server:9092");
	    	props.put("serializer.class", "kafka.serializer.StringEncoder");    
	   	 /* props.put("partitioner.class", "example.producer.SimplePartitioner");*/
	    	props.put("request.required.acks", "1");
	 
	    	ProducerConfig config = new ProducerConfig(props);
	    	Producer<String, String> producer = new Producer<String, String>(config);
	 
	    	/*	long runtime = new Date().getTime();  
	            String data1 = message.getData().toString()+ "|" + runtime;
	           	System.out.println(str.getData());
	           	String data1 = str.getData().toString();
	           	System.out.println(data1);   	
	           	KeyedMessage<String, String> data = new KeyedMessage<String, String>(message.getId()+"-"+message.getType(), message.getId(), data1);
	           */	
	           	
	           	ObjectMapper mapper = new ObjectMapper();

	           	//JSON to Object
	           	Message obj = mapper.readValue(str, Message.class);

	           	//JSON from String to Object
	           	System.out.println(mapper.writeValueAsString(obj.getData()));
	           	
	           	
	           	KeyedMessage<String, String> data = new KeyedMessage<String, String>(obj.getId()+obj.getType(), mapper.writeValueAsString(obj.getData()) );
	           	
	           	producer.send(data);
	        	producer.close();
		}
}
