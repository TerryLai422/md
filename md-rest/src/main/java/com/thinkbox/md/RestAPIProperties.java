package com.thinkbox.md;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "rest-api")
@PropertySource(value = "classpath:configuration/rest-api.yml", factory = YamlPropertySourceFactory.class)
public class RestAPIProperties {
	private Map<String, Map<String, Object>> services;

}
