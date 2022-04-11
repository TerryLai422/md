package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	@Autowired
	private RetrieverService retrieverService;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_RETRIEVE_INFO_DATA = "retrieve.info.data";
		
	private final String TOPIC_RETRIEVE_HISTORICAL_DATA = "retrieve.historical.data";
	
	private final String CONTAINER_FACTORY_MAP = "mapListener";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		logger.info("Sent topic: {} -> {}", topic, map);
		
		kafkaTemplate.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_RETRIEVE_INFO_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void processInfo(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_RETRIEVE_INFO_DATA, map);
		
		retrieverService.processInfo(map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_RETRIEVE_HISTORICAL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void processHistorical(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_RETRIEVE_HISTORICAL_DATA, map);
		
		retrieverService.processHistorical(map);
	}

}