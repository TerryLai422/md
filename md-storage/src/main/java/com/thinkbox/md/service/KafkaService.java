package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.model.Instrument;

import java.util.List;
import java.util.Map;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	@Autowired
	private StoreService storeService;
	
	@Autowired
	private MapKeyParameter mapKey;
	
	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_SAVE_EXCHANGE_DATA_LIST = "save.exchange.data.list";
	
	private final String TOPIC_SAVE_HISTORICAL_DATA_LIST = "save.historical.data.list";

	private final String TOPIC_SAVE_DETAIL_DATA = "save.detail.data";
	
	private final String TOPIC_DBGET_EXCHANGE_DATA = "dbget.exchange.data";
	
	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	public void publish(String topic, Map<String, Object> map) {
		logger.info(String.format("Sent topic: -> {}", map.toString()));
		kafkaTemplate.send(topic, map);
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_EXCHANGE_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveExchangeList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_SAVE_EXCHANGE_DATA_LIST, list.toString());
		storeService.saveInstrumentList(list);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveHistoricalList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_SAVE_HISTORICAL_DATA_LIST, list.toString());
		storeService.saveHistoricalList(list);
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_DETAIL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void saveDetail(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_SAVE_DETAIL_DATA, map.toString());
		storeService.saveInstrument(map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void getInstruments(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_DBGET_EXCHANGE_DATA, map.toString());
		String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();
		
		if (!subExchange.equals("-")) {
			List<Map<String, Object>> instruments = storeService.getInstruments(subExchange);
			instruments.forEach(System.out::println);
		}
	}
}