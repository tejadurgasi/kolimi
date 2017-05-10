package com.omniwyse.kafka.producer;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class SimpleProducer {
	public static void main(String[] args) {
    	long events = Long.parseLong(args[0]);
    	Random rnd = new Random();
 
    	Properties props = new Properties();
    	props.put("metadata.broker.list", "kafka-server:9092");
    	props.put("serializer.class", "kafka.serializer.StringEncoder");
   	/* props.put("partitioner.class", "example.producer.SimplePartitioner");*/
    	props.put("request.required.acks", "1");
 
    	ProducerConfig config = new ProducerConfig(props);
 
    	Producer<String, String> producer = new Producer<String, String>(config);
 
    	for (long nEvents = 0; nEvents < events; nEvents++) {
           	long runtime = new Date().getTime();  
           	String ip = "192.168.0." + rnd.nextInt(255);
           	String msg = runtime + ",www.example.com," + ip;
           	KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits1", ip, msg);
           	producer.send(data);
    	}
    	producer.close();
	}
}

