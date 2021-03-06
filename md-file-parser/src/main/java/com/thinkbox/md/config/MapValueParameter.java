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
@ConfigurationProperties(prefix = "map-value-parameter")
public class MapValueParameter {

	private String daily;
	
	private String weekly;
	
	private String monthly;
	
	private String info;
	
	private String detail;
	
	private String historical;
	
}