package com.example.bidder_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class BidderServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(BidderServiceApplication.class, args);
	}

}
