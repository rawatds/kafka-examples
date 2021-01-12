package com.dsr.kafka.ex03;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Supplier {

    private int supplierId;
    private String supplierName;
    private Date doj;
    private double creditLimit;

}
