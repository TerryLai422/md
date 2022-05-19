package com.thinkbox.md.model;

import org.springframework.data.mongodb.core.mapping.Document;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Document(collection = "dailysummary_etf")
public class DailySummaryETF extends DailySummary {
	
}