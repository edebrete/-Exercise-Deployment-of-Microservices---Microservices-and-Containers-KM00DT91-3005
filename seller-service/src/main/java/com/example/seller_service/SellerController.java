package com.example.seller_service;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class SellerController {

    private final SellerService service;

    @PostMapping("/sell")
    public String sell(@RequestParam String desc) {
        service.publishProduct(desc);
        return "Published";
    }

}