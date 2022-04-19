package com.thinkbox.md.model;

import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "instrument")
public class Instrument {

	@Id
	private String id;
	
	private String symbol;
	
	private String name;
	
	private String exchange;
	
	private String exchangeName;
	
	private String country;
	
	private String currency;
	
	private String industry;
	
	private String sector;
	
	private String type;
	
	private Double beta;
	
	private Double forwardPE;
	
	private Double priceToBook;
	
	private Long sharesOutstanding;
	
	private Long marketCap;
	
	private Map<String, Object> others;

}