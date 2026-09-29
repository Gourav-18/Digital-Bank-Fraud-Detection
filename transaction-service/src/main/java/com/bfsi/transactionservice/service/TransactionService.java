package com.bfsi.transactionservice.service;

import com.bfsi.transactionservice.client.AccountServiceClient;
import com.bfsi.transactionservice.dto.TransactionResponse;
import com.bfsi.transactionservice.dto.TransferRequest;
import com.bfsi.transactionservice.entity.Transaction;
import com.bfsi.transactionservice.entity.TransactionStatus;
import com.bfsi.transactionservice.entity.TransactionType;
import com.bfsi.transactionservice.event.TransactionCompletedEvent;
import com.bfsi.transactionservice.event.TransactionInitiatedEvent;
import com.bfsi.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor

public class TransactionService {
    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    //Kafka topics
    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /**
     * SAGA - Step 1 : Initiate Transfer
     * Deducts from sender via feign.
     * saves transaction as PROCESSING
     * publish event to kafka for fraud check.
     *
     * @param request
     * @return
     */
    public TransactionResponse transfer(@Valid TransferRequest request) {
        log.info("SAGA Start - Transfer : {} -> {} amount : {}", request.getSenderAccountNumber(), request.getReceiverAccountNumber(), request.getAmount());
        // SAGA - Step 1 Deducts from sender
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        // SAGA Step - 2 publish event for fraud detection.
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(), savedTransaction.getSenderAccountNumber(), savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(), savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent Published: {}", savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        return new TransactionResponse(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getType(),
                savedTransaction.getStatus(),
                savedTransaction.getDescription(),
                savedTransaction.getFailureReason(),
                savedTransaction.getReferenceNumber(),
                savedTransaction.getCreatedAt(),
                savedTransaction.getCompletedAt()
        );
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: " + transactionId
                )));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        List<Transaction> transactions = transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        List<TransactionResponse> list = transactions.stream().map(this::mapToResponse).toList();
        return list;
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for the Transaction: {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found with this id " + transactionId));
        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);
        if (storedOtp == null) {
//            OTP already expired.
            log.warn("OTP expired for Transaction : {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

//        User Entered incorrect OTP. So block account and refund.
        if (!storedOtp.equals(otp)) {
            // BLOCK ACCOUNT AND REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,
                    "Wrong OTP entered - transaction cancelled, " +
                            "account blocked for security");

            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);
        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );
        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);
        log.info("SAGA Completed - Transaction {} completed", transaction.getId());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
//        Publish Fraud detected -> Account service will block the account.
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked, Kindly contact to the bank.",transaction.getSenderAccountNumber());
//        SAGA Compensate to be called.
        compensateTransaction(transaction,reason);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.info("SAGA Compensation - Refunding: {} amount: {}", transaction.getSenderAccountNumber(), transaction.getAmount());
//        CREDIT Money back to sender Synchronously.
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + " - SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());
        transactionRepository.save(transaction);

//        PUBLISH Refund event - Notiofication service will alert user.
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA Compensation complete - {} refunded to: {}", transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction Not found with this id : "+transactionId));
        if (transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - Skipping",transactionId);
            return;
        }
        completeTransaction(transaction);
    }
}
