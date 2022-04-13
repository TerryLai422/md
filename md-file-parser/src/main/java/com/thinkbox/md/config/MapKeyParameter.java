package com.thinkbox.md.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "map-key-parameter")
public class MapKeyParameter {

	private String total;
	
	private String type;

	private String exchange;

	private String name;
		
	private String symbol;

	private String date;

	private String open;

	private String high;

	private String low;

	private String close;

	private String adjClose;

	private String volume;

	private String year;

	private String month;

	private String day;
	
	private String weekOfYear;
	
	private String yearForWeek;
	
	private String dayOfYear;
	
	private String dayOfWeek;

	
	// from
	private String fromDate;

	private String fromYear;

	private String fromMonth;

	private String fromDay;
	
	private String fromWeekOfYear;

	private String fromDayOfWeek;

	private String fromYearForWeek;

	
	// to
	private String toDate;

	private String toYear;

	private String toMonth;

	private String toDay;
	
	private String toWeekOfYear;

	private String toDayOfWeek;

	private String toYearForWeek;
	
}