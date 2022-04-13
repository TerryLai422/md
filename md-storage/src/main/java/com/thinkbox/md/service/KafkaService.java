package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	@Autowired
	private StoreService storageService;
	
	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_SAVE_EXCHANGE_DATA_LIST = "save.exchange.data.list";
	
	private final String TOPIC_SAVE_HISTORICAL_DATA_LIST = "save.historical.data.list";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	public void publish(String topic, Map<String, Object> map) {
		logger.info(String.format("Sent topic: -> {}", map.toString()));
		kafkaTemplate.send(topic, map);
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_EXCHANGE_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveExchangeList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_SAVE_EXCHANGE_DATA_LIST, list.toString());
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveHistoricalList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_SAVE_HISTORICAL_DATA_LIST, list.toString());
		storageService.saveHistoricalList(list);
	}

}