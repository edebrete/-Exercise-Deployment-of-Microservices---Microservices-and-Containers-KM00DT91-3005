package com.example.bidder_service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class BidderService {

    private final KafkaTemplate<String, BidEvent> kafka;
    private final BidRepository repo;
    private final Random random = new Random();
    private final String bidderId = UUID.randomUUID().toString();
    private int budget = random.nextInt(1000);

    @KafkaListener(topics = "products-for-sale", containerFactory = "productEventKafkaListenerFactory")
    public void onProduct(ProductForSaleEvent event) {
        System.out.println("\n#####\nBidder " + bidderId + " received event: " + event + "\n#####\n");

        int amount = event.initialPrice() + random.nextInt(100);
        if (amount > budget) {
            System. out.println("\n#####\nBidder " + bidderId + " skips auction for product " + event.description() + "\n#####\n");
            budget = random.nextInt(1000);
            System. out.println("\n#####\nBidder " + bidderId + " resets its budget." + "\n#####\n");
            return;
        }
        BidEntity bid = new BidEntity();
        bid.setBidderId(bidderId);
        bid.setProductId(event.productId());
        bid.setAmount(amount);
        repo.save(bid);
        budget = budget - amount;

        kafka.send("bids", new BidEvent(bidderId, event.productId(), event.description(), amount));
        System.out.println("\n#####\nBidder " + bidderId + " bids " + amount + "\n#####\n");
    }

}
