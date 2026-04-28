package com.example.auction_manager_service;

public record BidEvent(String bidderId, String productId, String description, int amount) {

}
