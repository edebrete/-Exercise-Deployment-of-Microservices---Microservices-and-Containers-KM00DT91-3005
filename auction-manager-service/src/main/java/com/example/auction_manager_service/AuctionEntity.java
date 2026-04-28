package com.example.auction_manager_service;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class AuctionEntity {

    @Id
    private String productId;

    private String winnerBidderId;
    private int winningAmount;

    public String getProductId() {
        return productId;
    }

    public String getWinnerBidderId() {
        return winnerBidderId;
    }

    public int getWinningAmount() {
        return winningAmount;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setWinnerBidderId(String winnerBidderId) {
        this.winnerBidderId = winnerBidderId;
    }

    public void setWinningAmount(int winningAmount) {
        this.winningAmount = winningAmount;
    }
}
