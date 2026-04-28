package com.example.auction_manager_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class AuctionManagerServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AuctionManagerServiceApplication.class, args);
	}

}
