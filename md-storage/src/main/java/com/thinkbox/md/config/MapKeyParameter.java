package com.thinkbox.md.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "map-key-parameter")
public class MapKeyParameter {

	private String wait;

	private String steps;
	
	private String next;
	
	private String total;
	
	private String type;

	private String exchange;
	
	private String subExchange;

	private String name;
		
	private String ticker;
	
	private String symbol;

	private String date;
	
	private String time;

	private String open;

	private String high;

	private String low;

	private String close;

	private String adjClose;

	private String prevClose;
	
	private String volume;

	private String percentChange;
	
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
	
	// suffix
	private String suffixValue;

	private String suffixSize;
	
	private String suffixHigh;
	
	private String suffixLow;
	
	private String historicalHigh;
	
	private String historicalLow;
	
	private String historicalHighDate;
	
	private String historicalLowDate;
	
	private String newHigh;
	
	private String newLow;
	
	// instrument
	
	private String exchangeName;
	
	private String country;
	
	private String currency;
	
	private String industry;
	
	private String sector;
	
	private String beta;
	
	private String forwardPE;
	
	private String priceToBook;
	
	private String sharesOutstanding;
	
	private String marketCap;
	
	private String dataType;
	
	private String dataSource;
	
	private String from;
	
	private String fileName;
	
	private String directory;
	
	private String historicalTotal;
	
	private String historicalFirstDate;
	
	private String historicalLastDate;
}