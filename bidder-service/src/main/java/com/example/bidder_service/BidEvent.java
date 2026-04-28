package com.example.bidder_service;

public record BidEvent(String bidderId, String productId, String description, int amount) {

}
