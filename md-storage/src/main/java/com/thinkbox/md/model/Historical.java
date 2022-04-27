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
@Document(collection = "historical")
public class Historical {

	@Id
	private String id;
	
	private String interval;
	
	private String symbol;
	
	private String ticker;
	
	private String date;
	
	private String time;

	private Integer year;
	
	private Integer month;
	
	private Integer day;
	
	private Integer weekOfYear;
	
	private Integer dayOfWeek;
	
	private Integer dayOfYear;
 
	private Double prevClose;
	
	private Double open;
	
	private Double high;
	
	private Double low;
	
	private Double close;
	
	private Double adjClose;
	
	private Long volume;
	
	private Map<String, Object> others;

}