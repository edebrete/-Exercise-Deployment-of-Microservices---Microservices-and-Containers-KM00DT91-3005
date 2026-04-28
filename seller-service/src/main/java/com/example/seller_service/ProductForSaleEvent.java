package com.example.seller_service;

public record ProductForSaleEvent(String productId, String description, int initialPrice) {

}