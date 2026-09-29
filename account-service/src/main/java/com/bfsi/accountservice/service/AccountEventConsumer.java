package com.bfsi.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountService accountService;
    /**
     * consume transaction.completed event from kafka.
     * Credits reciever accounts
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload){
        try {
            String reciverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting amount : {} to account: {}",amount, reciverAccount);
            accountService.creditBalance(reciverAccount, amount);
        } catch (Exception e) {
            log.info("Error crediting account:{}", e.getMessage());
        }
    }

    /**
     * consume fraud.detected event from kafka.
     * Credits reciever accounts
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload){
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud Detected - Blocking account: {}",accountNumber);
            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.info("Error blocking account:{}", e.getMessage());
        }
    }
}
