package com.bfsi.frauddetectionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {
    private final  FraudDetectionService fraudDetectionService;

    /**
     * Listens to "transaction.initiated" topic in kafka
     * Every transaction goes through fraud check before completing.
     * ba82b21c-d22a-4702-b0b5-69f8e3520241
     * @param payload
     */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(@Payload Map<String, Object> payload){
        log.info("Received transaction for fraud check: {}",payload.get("transactionId"));
        try {
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception e) {
            log.error("Error in fraud detection: {}", e.getMessage());
        }
    }
}
