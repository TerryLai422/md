package com.thinkbox.md.service;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		logger.info(STRING_LOGGER_SENT_MESSAGE, topic, map.toString());
		kafkaTemplate.send(topic, map);
	}

}
