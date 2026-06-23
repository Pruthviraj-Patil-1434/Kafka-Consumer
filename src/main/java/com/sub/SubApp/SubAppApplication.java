package com.sub.SubApp;

import com.sub.SubApp.config.AppConstants;
import com.sub.SubApp.model.Customer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class SubAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(SubAppApplication.class, args);
	}

	@KafkaListener(topics = AppConstants.TOPIC_NAME,groupId = "customerGroup")
	public void subscribeMsgFromKafkaTopic(Customer cxData){
		System.out.println("Message From Kafka Server");
		System.out.println(cxData);
	}

}
