package com.happy.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Rule {

    /**
     * rule id
     */
    private String id;

    /**
     * rule name
     */
    private String name;

    /**
     * rule type
     */
    private String type;

    /**
     * monitor measurement
     */
    private String measurement;

    /**
     * rule expression
     */
    private String expression;

    /**
     * measurement threshold
     */
    private String threshold;

    /**
     * alert level
     */
    private String level;

    /**
     * rule targetType
     */
    private String targetType;

    /**
     * rule targetId
     */
    private String targetId;

    /**
     * notice webhook, only DingDing group rebot here
     * TODO: more notice ways
     */
    private String webhook;
}