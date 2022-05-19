package com.thinkbox.md.config;

import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mongodb")
public class MultipleMongoProperties {

	private MongoProperties primary = new MongoProperties();
	private MongoProperties secondary = new MongoProperties();

	public MongoProperties getPrimary() {
		return primary;
	}

	public MongoProperties getSecondary() {
		return secondary;
	}
}
