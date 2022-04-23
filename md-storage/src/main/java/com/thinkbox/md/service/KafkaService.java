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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;
	@Autowired
	private StoreService storeService;

	@Autowired
	private MapKeyParameter mapKey;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_SAVE_EXCHANGE_LIST = "save.exchange.list";

	private final String TOPIC_SAVE_HISTORICAL_LIST = "save.historical.list";

	private final String TOPIC_SAVE_DETAIL_SINGLE = "save.detail.single";

	private final String TOPIC_DBGET_EXCHANGE_DATA = "dbget.exchange.data";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	public void publish(String topic, Map<String, Object> map) {
		logger.info(String.format("Sent topic: -> {}", map.toString()));
		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		logger.info("Sent topic: {} -> {}", topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_EXCHANGE_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveExchangeList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_SAVE_EXCHANGE_LIST, list.toString());
		storeService.saveInstrumentList(list);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveHistoricalList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_SAVE_HISTORICAL_LIST, list.toString());
		
		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);

		storeService.saveHistoricalList(list);
		
		if (topic != null) {
			publish(topic, list);
		} else {
			logger.info("Finish Last Step: {} {}", firstMap.toString());
		}
		
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_DETAIL_SINGLE, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveDetail(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_SAVE_DETAIL_SINGLE, list.toString());

		Map<String, Object> secondMap = list.get(1);

		storeService.saveInstrument(secondMap);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);

		if (topic != null) {
			List<Map<String, Object>> outputList = new ArrayList<>();
			outputList.add(firstMap);
			outputList.add(secondMap);

			publish(topic, outputList);
		} else {
			logger.info("Finish Last Step: {}", firstMap.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void getInstruments(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_EXCHANGE_DATA, map.toString());

		List<Map<String, Object>> outputList = null;
		String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();
		if (!subExchange.equals("-")) {
			outputList = storeService.getInstruments(subExchange);
		}

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {
				outputList.add(0, map);
				publish(topic, outputList);
			}
		} else {
			if (outputList != null) {
				outputList.forEach(System.out::println);
			}
			logger.info("Finish Last Step: {}", map.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void getHistorical(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_EXCHANGE_DATA, map.toString());

		List<Map<String, Object>> outputList = null;
		String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();
		if (!subExchange.equals("-")) {
			outputList = storeService.getInstruments(subExchange);
		}

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {
				outputList.add(0, map);
				publish(topic, outputList);
			}
		} else {
			if (outputList != null) {
				outputList.forEach(System.out::println);
			}
			logger.info("Finish Last Step: {}", map.toString());
		}
	}

	private String getTopicFromList(Map<String, Object> map) {
		Object objNext = map.get(mapKey.getNext());
		int next = Integer.valueOf(objNext.toString());

		Object objStep = map.get(mapKey.getSteps());

		@SuppressWarnings("unchecked")
		List<String> stepList = (List<String>) objStep;

		String topic = null;

		next++;
		if (stepList.size() > next) {
			topic = stepList.get(next);
			map.put(mapKey.getNext(), next);
		}
		return topic;
	}
}