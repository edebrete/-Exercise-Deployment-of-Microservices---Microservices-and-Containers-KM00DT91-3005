package com.example.auction_manager_service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;

@Service
@RequiredArgsConstructor
public class AuctionManager {

    private final AuctionRepository repo;
    private final Map<String, Integer> highestBids = new ConcurrentHashMap<>();
    private final Map<String, String> highestBidders = new ConcurrentHashMap<>();
    private final Map<String, Boolean> auctionClosed = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    @KafkaListener(topics = "bids", groupId = "auction-manager", containerFactory = "bidEventKafkaListenerFactory")
    public void onBid(BidEvent bid) {
        if (auctionClosed.getOrDefault(bid.productId(), false)) {
            System.out.println("\n#####\nAuctionManager received bid: " + bid + "\nBid ignored — auction already closed." + "\n#####\n");
            return;
        }

        System.out.println("\n#####\nAuctionManager received bid: " + bid + "\n#####\n");

        int currentBid = highestBids.getOrDefault(bid.productId(), 0);

        if (bid.amount() > currentBid) {
            highestBids.put(bid.productId(), bid.amount());
            highestBidders.put(bid.productId(), bid.bidderId());

            System.out.println("\n#####\nNew highest bid for " + bid.description() + ": " + bid.amount() + "\n#####\n");
        }

        timers.computeIfAbsent(bid.productId(), id ->
                scheduler.schedule(() -> closeAuction(bid.productId(), bid.description()), 10, TimeUnit.SECONDS)
        );
    }

    private void closeAuction(String productId, String description) {
        System.out.println("\n#####\nAuction closed for product: " + description + "\n#####\n");
        auctionClosed.put(productId, true);

        AuctionEntity auction = new AuctionEntity();
        auction.setProductId(productId);
        auction.setWinnerBidderId(highestBidders.get(productId));
        auction.setWinningAmount(highestBids.get(productId));
        repo.save(auction);

        System.out.println("\n#####\nThe auction winner is: " + auction.getWinnerBidderId() + "\n#####\n");
    }

}
