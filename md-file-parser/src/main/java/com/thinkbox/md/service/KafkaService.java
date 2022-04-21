package com.thinkbox.md.service;

import java.io.IOException;
import java.util.ArrayList;
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

	private final String TOPIC_PARSE_DETAIL_DATA_LIST = "parse.detail.data.list";

	private final String TOPIC_PARSE_DETAIL_DATA = "parse.detail.data";

	private final String TOPIC_PARSE_INFO_DATA = "parse.info.data";

	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";

	private final String TOPIC_PARSE_HISTORICAL_DATA = "parse.historical.data";

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
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_EXCHANGE_DATA, map.toString());

		try {
			String exchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			List<Map<String, Object>> outputList = fileParseService.parseExchangeFile(exchange);

			outputList.forEach(System.out::println);

			String topic = getTopicFromList(map);

			if (topic != null) {
				final Map<String, Object> first = outputList.remove(0);
				map.forEach((x, y) -> {
					first.put(x, y);
				});

				outputList.add(0, first);

				publish(topic, outputList);
			} else {
				logger.info("Finish Last Step: {}", map.get(mapKey.getSteps().toString()));
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DETAIL_DATA_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetailDataList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DETAIL_DATA_LIST, map.toString());

		try {
			final String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			List<String> list = fileParseService.getSymbols(subExchange);

			final String topic = getTopicFromList(map);

			list.stream().forEach(x -> {
				Map<String, Object> outputMap;
				try {
					outputMap = fileParseService.parseDetailFile(subExchange, x);

					if (topic != null) {
						List<Map<String, Object>> outputList = new ArrayList<>();

						outputList.add(0, map);
						outputList.add(outputMap);

						outputList.forEach(System.out::println);

						publish(topic, outputList);
					} else {
						logger.info(outputMap.toString());
						logger.info("Finish Last Step: {}", map.get(mapKey.getSteps().toString()));
					}
				} catch (IOException e) {
					logger.info(e.toString());
				}
			});
		} catch (

		IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DETAIL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetailData(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DETAIL_DATA, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			Map<String, Object> outputMap = fileParseService.parseDetailFile(subExchange, ticker);

			String topic = getTopicFromList(map);

			if (topic != null) {
				List<Map<String, Object>> outputList = new ArrayList<>();

				outputList.add(0, map);

				publish(topic, outputList);
			} else {
				logger.info(outputMap.toString());
				logger.info("Finish Last Step: {}", map.get(mapKey.getSteps().toString()));
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHistericalData(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_HISTORICAL_DATA, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();
			String dataType = map.getOrDefault(mapKey.getDataType(), "-").toString();
//			<List>String next = (List<String>) map.get("next");
			List<Map<String, Object>> list = fileParseService.parseHistoricalFile(dataType, symbol, ticker);
			list.forEach(System.out::println);
//			publish(next, list);
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_INFO_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseInfoData(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_INFO_DATA, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			Map<String, Object> outMap;
			outMap = fileParseService.parseInfoFile(ticker);
//			list.forEach(System.out::println);
//			publish(TOPIC_PROCESS_INFO_DATA, outMap);
		} catch (IOException e) {
			logger.info(e.toString());
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