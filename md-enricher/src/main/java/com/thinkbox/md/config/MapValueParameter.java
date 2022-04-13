package com.thinkbox.md.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "map-value-parameter")
public class MapValueParameter {

	private String daily;
	
	private String weekly;
	
	private String monthly;
	
}