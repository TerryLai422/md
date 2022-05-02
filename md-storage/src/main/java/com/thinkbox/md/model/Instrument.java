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
	
	private String market;
	
	private String symbol;
	
	private String ticker;
	
	private String name;
	
	private String exchange;
	
	private String subExch;
	
	private String exchangeN;
	
	private String country;
	
	private String currency;
		
	private String sector;
	
	private String type;
	
	private Double beta;
	
	private Double FPE;
	
	private Double PB;
	
	private Long sharesO;
	
	private Long mCap;
	
	private Long hTotal;
	
	private String hFirstD;
	
	private String hLastD;
	
	private Double hHigh;
	
	private Double hLow;
	
	private String hHighD;
	
	private String hLowD;
	
	private Double lastPrice;
		
	private Map<String, Object> group;

	private Map<String, Object> others;

}