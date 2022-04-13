package com.thinkbox.md.service;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.config.MapValueParameter;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplate;

	@Autowired
	private EnrichService enrichService;
	
	@Autowired
	private MapValueParameter mapValue;
	
	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";

	private final String TOPIC_PARSE_HISTORICAL_DATA = "parse.historical.data";

	private final String TOPIC_PROCESS_EXCHANGE_DATA_LIST = "process.exchange.data.list";

	private final String TOPIC_PROCESS_HISTORICAL_DATA_LIST = "process.historical.data.list";

	private final String TOPIC_SAVE_EXCHANGE_DATA_LIST = "save.exchange.data.list";

	private final String TOPIC_SAVE_HISTORICAL_DATA_LIST = "save.historical.data.list";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> map) {
		logger.info(String.format("Sent topic: {} -> {}", topic, map.toString()));

		kafkaTemplate.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PROCESS_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processHistericalData(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_PROCESS_HISTORICAL_DATA_LIST, list.toString());

//		List<Map<String, Object>> weeklyList = enrichService.consolidate(mapValue.getWeekly(), list);
//		weeklyList.forEach(System.out::println);
//		
//		List<Map<String, Object>> monthlyList = enrichService.consolidate(mapValue.getMonthly(), list);	
//		monthlyList.forEach(System.out::println);
		
		List<Map<String, Object>> outputList =  enrichService.enrich(list);
		outputList.forEach(System.out::println);
	}

}