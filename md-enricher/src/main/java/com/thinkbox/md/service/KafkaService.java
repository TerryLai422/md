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

	private final String TOPIC_PROCESS_EXCHANGE_DATA_LIST = "process.exchange.data.list";

	private final String TOPIC_PROCESS_HISTORICAL_DATA_LIST = "process.historical.data.list";

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
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PROCESS_EXCHANGE_DATA_LIST, list.toString());

		List<Map<String, Object>> outputList = enrichService.enrichExchange(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);

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
			logger.info("Finish Last Step: {}", firstMap.get(mapKey.getSteps().toString()));
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PROCESS_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processHistericalData(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PROCESS_HISTORICAL_DATA_LIST, list.toString());

//		List<Map<String, Object>> weeklyList = enrichService.consolidate(mapValue.getWeekly(), list);
//		weeklyList.forEach(System.out::println);
//		
//		List<Map<String, Object>> monthlyList = enrichService.consolidate(mapValue.getMonthly(), list);	
//		monthlyList.forEach(System.out::println);

		List<Map<String, Object>> outputList = enrichService.enrichHistorical(list);
		outputList.forEach(System.out::println);
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