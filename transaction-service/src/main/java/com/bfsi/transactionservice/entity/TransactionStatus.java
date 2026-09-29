package com.bfsi.transactionservice.entity;

/**
 * Transaction lifecycle flow
 *
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION (suspicious detected)
 *                                  -> COMPLETED (verified)
 *                                  -> FLAGGED (SAGA Refund)
 *                       -> FAILED
 *                       -> FLAGGED
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    PENDING_VERIFICATION,
    FLAGGED,
    FAILED
}
