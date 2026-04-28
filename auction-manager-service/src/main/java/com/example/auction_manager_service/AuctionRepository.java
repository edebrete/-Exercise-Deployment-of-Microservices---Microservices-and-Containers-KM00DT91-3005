package com.example.auction_manager_service;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AuctionRepository extends JpaRepository<AuctionEntity, String> {

}
