package com.training.statementservice.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {

    private Long id;
    private String accountNo;
    private String transactionType;
    private BigDecimal amount;
    private String description;

}
