package com.thinkbox.md.model;

import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Document(collection = "analysis_etf")
public class AnalysisETF extends Analysis {
	
}