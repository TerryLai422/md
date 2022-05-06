package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.config.MapKeyParameter;

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
	private RetrieveService retrieveService;

	@Autowired
	private MapKeyParameter mapKey;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_RETRIEVE_YAHOO_SINGLE = "retrieve.yahoo.single";
	
	private final String TOPIC_RETRIEVE_YAHOO_LIST = "retrieve.yahoo.list";
	
	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";
	
	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		logger.info(STRING_LOGGER_SENT_MESSAGE, topic, map);
		
		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_SENT_MESSAGE, topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_RETRIEVE_YAHOO_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void retreiveYahoo(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_RETRIEVE_YAHOO_SINGLE, map);
		
		retrieveService.retrieveYahoo(map);
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_RETRIEVE_YAHOO_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void retrieveYahooList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_RETRIEVE_YAHOO_LIST, list.toString());

		Map<String, Object> firstMap = list.get(0);
		
		String topic = getTopicFromList(firstMap);

		List<Map<String, Object>> outputList = retrieveService.retrieveYahooList(list);
		outputList.forEach(System.out::println);

		if (topic != null) {
			
			final Map<String, Object> first = outputList.remove(0);
			firstMap.forEach((x, y) -> {
				first.put(x, y);
			});

			outputList.add(0, first);

			outputList.forEach(System.out::println);

			publish(topic, outputList);

		} else {
			outputList.forEach(System.out::println);
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
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