package com.example.bidder_service;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

@Entity
public class BidEntity {

    @Id
    @GeneratedValue
    private Long id;

    private String bidderId;
    private String productId;
    private int amount;

    public void setId(Long id) {
        this.id = id;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setBidderId(String bidderId) {
        this.bidderId = bidderId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
}
