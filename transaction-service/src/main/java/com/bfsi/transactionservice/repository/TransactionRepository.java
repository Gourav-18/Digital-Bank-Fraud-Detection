package com.bfsi.transactionservice.repository;

import com.bfsi.transactionservice.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TransactionRepository extends JpaRepository<Transaction, String> {
    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String senderAccountNumber);
}
