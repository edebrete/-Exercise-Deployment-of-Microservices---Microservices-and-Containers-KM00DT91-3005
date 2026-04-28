package com.example.seller_service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class SellerService {

    private final ProductRepository repo;
    private final KafkaTemplate<String, ProductForSaleEvent> kafka;
    private final Random random = new Random();

    public void publishProduct(String description) {

        int price = 50 + random.nextInt(50);

        Product p = new Product();
        p.setId(UUID.randomUUID().toString());
        p.setDescription(description);
        p.setInitialPrice(price);
        repo.save(p);

        kafka.send("products-for-sale", new ProductForSaleEvent(p.getId(), description, price));
        System.out.println("\n#####\nPublishing product " + description + " to Kafka...\n#####\n");

    }

}