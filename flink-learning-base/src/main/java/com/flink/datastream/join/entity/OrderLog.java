package com.flink.datastream.join.entity;

import lombok.Data;

@Data
public class OrderLog {
    private String goodId;
    private String goodName;
    // default eventTime for test only
    private Long timeStamp = System.currentTimeMillis();
}