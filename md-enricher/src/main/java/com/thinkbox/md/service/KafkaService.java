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

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;

	@Autowired
	private EnrichService enrichService;

	@Autowired
	private MapKeyParameter mapKey;

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
	public void publish(String topic, List<Map<String, Object>> list) {
		logger.info("Sent topic: {} -> {}", topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PROCESS_EXCHANGE_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processExchangeData(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> list: {}", TOPIC_PROCESS_EXCHANGE_DATA_LIST, list.toString());

		Map<String, Object> firstMap = list.get(0);

		Object objNext = firstMap.get(mapKey.getNext());
		int next = Integer.valueOf(objNext.toString());

		String topic = getTopicFromList(firstMap, next);

		List<Map<String, Object>> outputList = enrichService.enrichExchange(list);
		outputList.forEach(System.out::println);

		if (topic != null) {
			final Map<String, Object> first = outputList.remove(0);
			firstMap.forEach((x, y) -> {
				first.put(x, y);
			});

			next++;
			first.put(mapKey.getNext(), next);
			outputList.add(0, first);

			outputList.forEach(System.out::println);

			publish(topic, outputList);
		} else {
			logger.info("Topic is null");
			outputList.forEach(System.out::println);
		}
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

		List<Map<String, Object>> outputList = enrichService.enrichHistorical(list);
		outputList.forEach(System.out::println);
	}

	private String getTopicFromList(Map<String, Object> map, int next) {

		Object objStep = map.get(mapKey.getSteps());

		@SuppressWarnings("unchecked")
		List<String> stepList = (List<String>) objStep;

		String topic = null;

		next++;
		if (stepList.size() > next) {
			topic = stepList.get(next);
		}
		return topic;
	}
}