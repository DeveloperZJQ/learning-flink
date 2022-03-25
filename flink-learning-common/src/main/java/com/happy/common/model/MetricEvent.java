package com.happy.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

	/**
	 * Metric name
	 */
	private String name;

	/**
	 * Metric name
	 */
	private String topic;

	/**
	 * Metric timestamp
	 */
	private Long timestamp;

	/**
	 * Metric fields
	 */
	private Map<String, Object> fields;

	/**
	 * Metric tags
	 */
	private Map<String, String> tags;
}