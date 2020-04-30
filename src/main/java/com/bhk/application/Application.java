package com.bhk.application;

import com.bhk.consumer.MyConsumer;

public class Application {

	public static void main(String[] args) {
		Application app = new Application();
		try {
			app.executeKafkaProducer();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
	public void executeKafkaProducer() throws Exception{
		MyConsumer consumer = new MyConsumer();
		consumer.createConsumer();
		consumer.getRecord();
	}
}
