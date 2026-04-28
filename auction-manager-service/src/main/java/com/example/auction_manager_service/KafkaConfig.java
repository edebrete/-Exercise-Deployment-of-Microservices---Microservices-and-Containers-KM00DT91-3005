package com.example.auction_manager_service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.consumer.group-id:auction-manager-group}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, BidEvent> bidEventConsumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        config.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, BidEvent.class);
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.example.*");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = "bidEventKafkaListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, BidEvent> bidEventKafkaListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, BidEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(bidEventConsumerFactory());
        return factory;
    }
}

