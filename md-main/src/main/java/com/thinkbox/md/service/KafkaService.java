package com.thinkbox.md.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaService {
	
	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, map.toString());
		kafkaTemplate.send(topic, map);
	}

}
