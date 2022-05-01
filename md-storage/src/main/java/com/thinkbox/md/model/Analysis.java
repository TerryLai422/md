package com.thinkbox.md.model;

import java.util.Map;

import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Document(collection = "analysis")
public class Analysis extends Historical {

	private Map<String, Object> instrument;
	
	private Map<String, Object> indicator;
	
}