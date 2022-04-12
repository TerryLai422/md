package com.thinkbox.md.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "parameter")
public class ParameterProperties {

	private String keyType;

	private String keySymbol;

	private String keyDate;

	private String keyOpen;

	private String keyHigh;

	private String keyLow;

	private String keyClose;

	private String keyAdjClose;

	private String keyVolume;

	private String keyYear;

	private String keyMonth;

	private String keyDay;
	
	private String keyWeekOfYear;
	
	private String keyYearForWeek;
	
	private String valueDaily;
	
	private String valueWeekly;
	
	private String valueMonthly;

}