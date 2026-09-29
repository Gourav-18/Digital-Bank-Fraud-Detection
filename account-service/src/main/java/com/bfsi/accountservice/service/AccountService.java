package com.bfsi.accountservice.service;

import com.bfsi.accountservice.dto.AccountResponse;
import com.bfsi.accountservice.dto.CreateAccountRequest;
import com.bfsi.accountservice.entity.Account;
import com.bfsi.accountservice.entity.AccountStatus;
import com.bfsi.accountservice.entity.AccountType;
import com.bfsi.accountservice.repository.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(@Valid CreateAccountRequest request) {
        log.info("Creating account for {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exists for email: "+request.getEmail());
        }

        Account acc = new Account();
        acc.setAccountHolderName(request.getAccountHolderName());
        acc.setEmail(request.getEmail());
        acc.setPhone(request.getPhone());
        acc.setAccountType(request.getAccountType());
        acc.setStatus(AccountStatus.ACTIVE);
        acc.setBalance(request.getInitialDeposit());
        acc.setAccountNumber(generateAccountNumber());
        acc.setDailyTransactionLimit(request.getAccountType() == AccountType.SAVINGS ? new BigDecimal("100000") : new BigDecimal("500000"));

        Account savedAccount = accountRepository.save(acc);
        log.info("Account created : {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

//    Generate a 12 digit account number
    private String generateAccountNumber() {
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        }while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account savedAccount) {
        AccountResponse response = new AccountResponse();
        response.setId(savedAccount.getId());
        response.setAccountNumber(savedAccount.getAccountNumber());
        response.setAccountHolderName(savedAccount.getAccountHolderName());
        response.setEmail(savedAccount.getEmail());
        response.setPhone(savedAccount.getPhone());
        response.setAccountType(savedAccount.getAccountType());
        response.setStatus(savedAccount.getStatus());
        response.setBalance(savedAccount.getBalance());
        response.setDailyTransactionLimit(savedAccount.getDailyTransactionLimit());
        response.setCreatedAt(savedAccount.getCreatedAt());
        return response;
    }

    /**
     * Get Account by account number.
     * @param accountNumber
     * @return
     */
    public AccountResponse getAccount(String accountNumber) {
        Account acc = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        return mapToResponse(acc);
    }

    /**
     * Get account balance using account number.
     * @param accountNumber
     * @return
     */
    public BigDecimal getBalance(String accountNumber) {
        Account acc = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        return acc.getBalance();
    }

    /**
     * Block account - called by fraud detection service via kafka.
     * @param accountNumber
     */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account : {}",accountNumber);
        Account acc = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        acc.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(acc);
        log.info("Account blocked : {}",accountNumber);
    }

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance {} from account : {}", amount, accountNumber);
        Account acc = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        if (acc.getStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account is not active "+accountNumber);
        }

        if(acc.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient funds for account "+accountNumber);
        }

        acc.setBalance(acc.getBalance().subtract(amount));
        accountRepository.save(acc);
        log.info("Balance updated. New Balance : {}", acc.getBalance());

    }

    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting balance {} to account : {}", amount, accountNumber);
        Account acc = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        acc.setBalance(acc.getBalance().add(amount));
        accountRepository.save(acc);
        log.info("Balance updated. New Balance : {}", acc.getBalance());

    }
}
