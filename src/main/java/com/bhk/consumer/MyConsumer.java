package com.bhk.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyConsumer {
	Properties prop = new Properties();
	Properties kafkaProp = new Properties();
	KafkaConsumer<String, String> consumer = null;

	public MyConsumer() throws IOException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		prop.load(cl.getResourceAsStream("application.properties"));
		// Creating consumer properties
		System.out.println(prop.getProperty("kafka.bootstrapServer"));
		kafkaProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("kafka.bootstrapServer"));
		kafkaProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("kafka.group_id"));
		kafkaProp.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	public void createConsumer() {
		// creating consumer
		consumer = new KafkaConsumer<String, String>(kafkaProp);
	}

	public int getRecord(){
		Logger logger = LoggerFactory.getLogger(MyConsumer.class.getName());		
		String topic = prop.getProperty("kafka.topicName");
		System.out.println("topic :: "+topic);		
		// Subscribing
		consumer.subscribe(Arrays.asList(topic));
		// polling
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key() + ", Value:" + record.value());
				logger.info("Partition:" + record.partition() + ",Offset:" + record.offset());
			}

		}
	}
}
