package com.thinkbox.md.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
	
	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);
	
	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplate;

	@Autowired
	private FileParserService fileParserService;
	
	private final String ASYNC_EXECUTOR = "asyncExecutor";
	
	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";
	
	private final String TOPIC_PARSE_HISTORICAL_DATA = "parse.historical.data";
	
	private final String TOPIC_SAVE_EXCHANGE_DATA_LIST = "save.exchange.data.list";
	
	private final String TOPIC_SAVE_HISTORICAL_DATA_LIST = "save.historical.data.list";
	
	private final String CONTAINER_FACTORY_MAP = "mapListener";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> map) {
		logger.info(String.format("Sent topic: {} -> {}", topic, map.toString()));
		
		kafkaTemplate.send(topic, map);
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseExchangeFile(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_PARSE_EXCHANGE_DATA, map.toString());
		
		try {
			String exchange = map.getOrDefault("exchange", "-").toString();
			List<Map<String, Object>> list;
			list = fileParserService.parseExchangeFile(exchange);
			list.forEach(System.out::println);
			kafkaTemplate.send(TOPIC_SAVE_EXCHANGE_DATA_LIST, list);
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void processHistericalData(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_PARSE_HISTORICAL_DATA, map.toString());
		
		try {
			String symbol = map.getOrDefault("symbol", "-").toString();
			List<Map<String, Object>> list;
			list = fileParserService.parseHistoricalFile(symbol);
			list.forEach(System.out::println);
			publish(TOPIC_SAVE_HISTORICAL_DATA_LIST, list);
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}
	
}