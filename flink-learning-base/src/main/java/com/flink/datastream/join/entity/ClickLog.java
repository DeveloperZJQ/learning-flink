package com.flink.datastream.join.entity;

import lombok.Data;

@Data
public class ClickLog {
    private String sessionId;
    private String goodId;
    // default eventTime for test only
    private Long timeStamp = System.currentTimeMillis();
}