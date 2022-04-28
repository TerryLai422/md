package com.thinkbox.md.model;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class HistoricalSummary {

	@Id
	private String ticker;
	
	private Long total;
	
	private String firstDate;
	
	private String lastDate;
	
	private String high;
	
	private String low;
}
