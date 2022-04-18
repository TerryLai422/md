package com.thinkbox.md.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.config.MapKeyParameter;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;

	@Autowired
	private MapKeyParameter mapKey;
	
	@Autowired
	private FileParseService fileParseService;

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_PARSE_INFO_DATA = "parse.info.data";

	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";

	private final String TOPIC_PARSE_HISTORICAL_DATA = "parse.historical.data";

	private final String TOPIC_PROCESS_INFO_DATA = "process.info.data";

	private final String TOPIC_PROCESS_EXCHANGE_DATA_LIST = "process.exchange.data.list";

	private final String TOPIC_PROCESS_HISTORICAL_DATA_LIST = "process.historical.data.list";

	private final String TOPIC_SAVE_EXCHANGE_DATA_LIST = "save.exchange.data.list";

	private final String TOPIC_SAVE_HISTORICAL_DATA_LIST = "save.historical.data.list";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		logger.info("Sent topic: {} -> {}", topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		logger.info("Sent topic: {} -> {}", topic, map.toString());

		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseExchangeData(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_PARSE_EXCHANGE_DATA, map.toString());

		try {
			Object objNext = map.get(mapKey.getNext());
			int next = Integer.valueOf(objNext.toString());

			String topic = getTopicFromList(map, next);

			String exchange = map.getOrDefault(mapKey.getExchange(), "-").toString();

			List<Map<String, Object>> outputList = fileParseService.parseExchangeFile(exchange);

			if (topic != null) {
				final Map<String, Object> first = outputList.remove(0);
				map.forEach((x, y) -> {
					first.put(x, y);
				});
				
				next++;				
				first.put(mapKey.getNext(), next);
				outputList.add(0, first);

				outputList.forEach(System.out::println);

				publish(topic, outputList);
			} else {
				outputList.forEach(System.out::println);				
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHistericalData(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_PARSE_HISTORICAL_DATA, map.toString());

		try {
			String symbol = map.getOrDefault("symbol", "-").toString();
//			<List>String next = (List<String>) map.get("next");
			List<Map<String, Object>> list = fileParseService.parseHistoricalFile(symbol);
//			list.forEach(System.out::println);
//			publish(next, list);
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_INFO_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseInfoData(Map<String, Object> map) {
		logger.info("Received topic: {} -> map: {}", TOPIC_PARSE_INFO_DATA, map.toString());

		try {
			String symbol = map.getOrDefault("symbol", "-").toString();
			Map<String, Object> outMap;
			outMap = fileParseService.parseInfoFile(symbol);
//			list.forEach(System.out::println);
			publish(TOPIC_PROCESS_INFO_DATA, outMap);
		} catch (IOException e) {
			logger.info(e.toString());
		}
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