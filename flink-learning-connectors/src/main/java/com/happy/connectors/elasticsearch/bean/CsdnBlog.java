package com.happy.connectors.elasticsearch.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CsdnBlog {

	private String author;
	private String titile;
	private String tag;
	private String content;
	private String view;
	private String date;
}