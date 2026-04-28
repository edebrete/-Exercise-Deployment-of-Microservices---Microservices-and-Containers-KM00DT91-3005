package com.example.bidder_service;

public record ProductForSaleEvent (String productId, String description, int initialPrice) {

}
