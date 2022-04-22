package com.thinkbox.md.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

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

	private final String TOPIC_PARSE_HISTORICAL_DATA_LIST = "parse.historical.data.list";

	private final String TOPIC_PARSE_DETAIL_DATA_LIST = "parse.detail.data.list";

	private final String TOPIC_PARSE_DETAIL_DATA = "parse.detail.data";

	private final String TOPIC_PARSE_INFO_DATA = "parse.info.data";

	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";

	private final String TOPIC_PARSE_HISTORICAL_DATA = "parse.historical.data";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final int BATCH_SAVE_LIMIT = 2000;

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
	public void parseExchange(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_EXCHANGE_DATA, map.toString());

		try {
			String exchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			List<Map<String, Object>> outputList = fileParseService.parseExchangeFile(exchange);

			outputList.forEach(System.out::println);

			String topic = getTopicFromList(map);

			if (topic != null) {
				final Map<String, Object> firstMap = outputList.remove(0);
				map.forEach((x, y) -> {
					firstMap.put(x, y);
				});

				outputList.add(0, firstMap);

				publish(topic, outputList);
			} else {
				logger.info("Finish Last Step: {}", map.get(mapKey.getSteps().toString()));
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHistoricalList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_HISTORICAL_DATA_LIST, map.toString());

		try {
			final String misc = map.getOrDefault(mapKey.getMisc(), "-").toString();

			List<Map<String, Object>> list = fileParseService.getSymbolsfromHistoricalDirectory(misc);
			list.forEach(System.out::println);

			list.stream().parallel().forEach(x -> {

				map.forEach((i, j) -> {
					x.put(i, j);
				});

				parseHistericalData(x);
			});

		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DETAIL_DATA_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetailList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DETAIL_DATA_LIST, map.toString());

		try {
			final String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			List<String> list = fileParseService.getSymbolsfromDetailDirectory(subExchange);

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
	public void parseDetail(Map<String, Object> map) {
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
	public void parseHisterical(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_HISTORICAL_DATA, map.toString());

		parseHistericalData(map);
	}

	private void parseHistericalData(Map<String, Object> map) {
		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();
			String dataType = map.getOrDefault(mapKey.getDataType(), "-").toString();
			String misc = map.getOrDefault(mapKey.getMisc(), "-").toString();

			final String topic = getTopicFromList(map);

			// <List>String next = (List<String>) map.get("next");
			List<Map<String, Object>> outputList = fileParseService.parseHistoricalFile(misc, dataType, symbol, ticker);

			if (topic != null) {
//				outputList.forEach(System.out::println);
				logger.info("Number of records:" + outputList.size());
				if (outputList.size() >= 2) {
					final Map<String, Object> firstMap = outputList.remove(0);
					map.forEach((i, j) -> {
						firstMap.put(i, j);
					});

					int size = outputList.size();

					if (size <= BATCH_SAVE_LIMIT) {
//					System.out.println("less than limit");
						outputList.add(0, firstMap);
						publish(topic, outputList);
					} else {
//					System.out.println("greater than limit");
						List<Map<String, Object>> subList = null;
						for (int i = 0; i < size; i += BATCH_SAVE_LIMIT) {
							subList = outputList.stream().skip(i).limit(BATCH_SAVE_LIMIT).map(y -> y)
									.collect(Collectors.toList());
//						logger.info("Number of records (sublist):" + subList.size());
							subList.add(0, firstMap);

							publish(topic, subList);
						}
					}
				}
			} else {
				logger.info(outputList.toString());
				logger.info("Finish Last Step: {}", map.get(mapKey.getSteps().toString()));
			}

		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_INFO_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseInfo(Map<String, Object> map) {
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