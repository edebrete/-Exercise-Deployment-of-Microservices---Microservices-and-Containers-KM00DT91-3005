package com.example.seller_service;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Product {

    @Id
    private String id;

    private String description;
    private int initialPrice;

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public int getInitialPrice() {
        return initialPrice;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setInitialPrice(int initialPrice) {
        this.initialPrice = initialPrice;
    }
}