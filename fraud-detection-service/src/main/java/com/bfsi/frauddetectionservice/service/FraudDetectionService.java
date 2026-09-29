package com.bfsi.frauddetectionservice.service;

import com.bfsi.frauddetectionservice.client.AccountServiceClient;
import com.bfsi.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionsPerMinute;
    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;
    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());
//        Fetch real balance from Account service
        BigDecimal senderBal = accountServiceClient.getBalance(accountNumber);
        log.info("checking transaction: {} account : {} amount : {} balance : {}", transactionId, accountNumber, amount, senderBal);

        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBal);
        if (result.isFraud()) {
            log.info("Suspicious activity detected - account: {} reason: {} - requesting OTP verification", accountNumber, result.getReason());
            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        } else {
            log.info("Transaction Clean");
            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("reason", null);
            transactionCleanEvent.put("isFraud", false);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
        }

    }

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal senderBal) {
        // Pattern 1: Velocity Check
        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(
                    true, "Too many transactions in 60 seconds " +
                    "- Velocity limit exceeded");
        }

        // Pattern 2: Amount check
        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(
                    true, "Unusual transaction amount " +
                    "- exceeds 5x your average");
        }

        // Pattern 3: Balance Check
        if (senderBal.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBal, amount)) {
            return new FraudCheckResult(
                    true, "Transaction exceed 90% of account balance");
        }

        return new FraudCheckResult(false, null);
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBal, BigDecimal amount) {
        BigDecimal maxAllowed = senderBal.multiply(
                BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxAllowed: {} suspicious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avg_amount:" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);
        if (avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }
        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(
                BigDecimal.valueOf(suspiciousAmountMultiplier));
        // Update running average
        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(avgKey, newAvg.toString());
        log.info("Amount check - amount: {} threshold: {} suspicious: {}",
                amount, threshold, amount.compareTo(threshold) > 0);
        return amount.compareTo(threshold) > 0;
    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account: {} count: {}/{}",
                accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
            /*
        1. Method definition
private boolean isVelocityExceeded(String accountNumber) {

It takes an account number and returns:

true  → velocity limit exceeded
false → velocity limit not exceeded

For example, suppose:

maxTransactionsPerMinute = 5;

Then the account is allowed up to 5 transactions in the 60-second window.

2. Create a Redis key
String key = "fraud:velocity:" + accountNumber;

Suppose:

accountNumber = "ACC123"

The Redis key becomes:

fraud:velocity:ACC123

This gives each account its own counter.

For example:

fraud:velocity:ACC123 → 3
fraud:velocity:ACC456 → 7
fraud:velocity:ACC789 → 1

So transactions from different accounts don't share the same counter.

3. Increment the counter
Long count = redisTemplate.opsForValue().increment(key);

This is the most important line.

Redis has a command conceptually equivalent to:

INCR fraud:velocity:ACC123

Every time this method is called, Redis increments the value by 1.

First transaction

The key doesn't exist:

fraud:velocity:ACC123

Redis creates it:

fraud:velocity:ACC123 = 1

count becomes:

1
Second transaction
1 → 2

count = 2

Third transaction
2 → 3

count = 3

And so on.

4. Why count == 1?
if(count != null && count == 1){
    redisTemplate.expire(key, 60, TimeUnit.SECONDS);
}

This means:

When the counter is created for the first transaction, set its expiration to 60 seconds.

So the first transaction creates:

fraud:velocity:ACC123 = 1

Then:

TTL = 60 seconds

Redis now looks conceptually like:

Key                         Value       TTL
------------------------------------------------
fraud:velocity:ACC123       1           60 sec

Every subsequent transaction increments the same key:

1 → 2 → 3 → 4 → 5

The TTL continues counting down.

After 60 seconds, Redis automatically deletes the key:

fraud:velocity:ACC123
             ↓
          deleted

The next transaction starts a new window:

1
5. Logging
log.info("Velocity check - account: {} count: {}/{}",
        accountNumber, count, maxTransactionsPerMinute);

Suppose:

accountNumber = "ACC123"
count = 4
maxTransactionsPerMinute = 5

Your log will look approximately like:

Velocity check - account: ACC123 count: 4/5

This is useful for monitoring/debugging.

6. Check whether the limit has been exceeded
return count != null && count > maxTransactionsPerMinute;

Suppose:

maxTransactionsPerMinute = 5;

Then:

Transaction	Redis count	Result
1	1	false
2	2	false
3	3	false
4	4	false
5	5	false
6	6	true
7	7	true

Notice your condition is:

count > maxTransactionsPerMinute

not:

count >= maxTransactionsPerMinute

Therefore the 5th transaction is allowed, and the 6th transaction exceeds the limit.

Why use Redis for this?

Imagine you have multiple instances of your transaction service:

                    ┌── Transaction Service #1
                    │
User → API Gateway ─┼── Transaction Service #2
                    │
                    └── Transaction Service #3
                              │
                              ↓
                            Redis

If you stored the counter in Java memory:

int count;

each application instance would have its own counter.

For example:

Server 1 → 2 transactions
Server 2 → 2 transactions
Server 3 → 2 transactions

The application might incorrectly think the account made only 2 transactions on each server.

Redis gives all instances a shared counter:

Server 1 ─┐
Server 2 ─┼──→ Redis → fraud:velocity:ACC123 = 6
Server 3 ─┘

That's why Redis is very useful for rate limiting and fraud velocity checks.
         */
    }
}
