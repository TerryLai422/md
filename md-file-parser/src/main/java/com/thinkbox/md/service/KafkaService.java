package com.thinkbox.md.service;

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
	FileParserService fileParserService;
	
	@Async("asyncExecutor")
	@KafkaListener(topics = "parse.exchange.data", containerFactory = "mapListener")
	public void parseExchangeFile(Map<String, Object> map) {
		logger.info("topic: parse.exchange.data - map: {}", map);
		String exchange = map.getOrDefault("exchange", "-").toString();
		List<Map<String, Object>> list = fileParserService.parseExchangeFile(exchange);
		list.forEach(System.out::println);
	}

	@Async("asyncExecutor")
	@KafkaListener(topics = "parse.historical.data", containerFactory = "mapListener")
	public void processHistericalData(Map<String, Object> map) {
		logger.info("receive topic: parse.historical.data - map: {}", map);
		String symbol = map.getOrDefault("symbol", "-").toString();
		List<Map<String, Object>> list = fileParserService.parseHistoricalFile(symbol);
		list.forEach(System.out::println);
		kafkaTemplate.send("save.historical.data", list);
	}
	
}