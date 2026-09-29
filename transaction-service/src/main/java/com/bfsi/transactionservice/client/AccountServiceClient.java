package com.bfsi.transactionservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {
    @PutMapping("/api/v1/accounts/{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber,
                         @RequestBody BigDecimal amount);

    @PutMapping("/api/v1/accounts/{accountNumber}/credit")
    String creditBalance(@PathVariable String accountNumber,
                         @RequestBody BigDecimal amount);
}
