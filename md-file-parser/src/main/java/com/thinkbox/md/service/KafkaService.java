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

	private final String TOPIC_PARSE_DAILY_LIST = "parse.daily.list";

	private final String TOPIC_PARSE_DAILY_SINGLE = "parse.daily.single";

	private final String TOPIC_PARSE_HISTORICAL_LIST = "parse.historical.list";

	private final String TOPIC_PARSE_DETAIL_LIST = "parse.detail.list";

	private final String TOPIC_PARSE_DETAIL_SINGLE = "parse.detail.single";

	private final String TOPIC_PARSE_INFO_SINGLE = "parse.info.single";

	private final String TOPIC_PARSE_EXCHANGE_DATA = "parse.exchange.data";

	private final String TOPIC_PARSE_HISTORICAL_SINGLE = "parse.historical.single";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final int BATCH_LIMIT = 2000;

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
				logger.info("Finish Last Step: {}", map.toString());
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DETAIL_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetailList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DETAIL_LIST, map.toString());

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
						logger.info("Finish Last Step: {}", map.toString());
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
	@KafkaListener(topics = TOPIC_PARSE_DETAIL_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetail(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DETAIL_SINGLE, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();

			Map<String, Object> outputMap = fileParseService.parseDetailFile(subExchange, ticker);

			String topic = getTopicFromList(map);

			if (topic != null) {
				List<Map<String, Object>> outputList = new ArrayList<>();

				outputList.add(0, map);
				outputList.add(1, outputMap);
				publish(topic, outputList);
			} else {
				logger.info(outputMap.toString());
				logger.info("Finish Last Step: {}", map.toString());
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHistoricalList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_HISTORICAL_LIST, map.toString());

		try {
			final String directory = map.getOrDefault(mapKey.getDirectory(), "-").toString();

			List<Map<String, Object>> list = fileParseService.getSymbolsfromHistoricalDirectory(directory);
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
	@KafkaListener(topics = TOPIC_PARSE_HISTORICAL_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHisterical(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_HISTORICAL_SINGLE, map.toString());

		parseHistericalData(map);
	}

	private void parseHistericalData(Map<String, Object> map) {
		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
			String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();
			String dataSource = map.getOrDefault(mapKey.getDataSource(), "-").toString();
			String directory = map.getOrDefault(mapKey.getDirectory(), "-").toString();

			final String topic = getTopicFromList(map);

			// <List>String next = (List<String>) map.get("next");
			List<Map<String, Object>> outputList = fileParseService.parseHistoricalFile(directory, dataSource, symbol,
					ticker);

			if (topic != null) {
				logger.info("Number of records:" + outputList.size());
				if (outputList.size() >= 2) {
					final Map<String, Object> firstMap = outputList.remove(0);
					map.forEach((i, j) -> {
						firstMap.put(i, j);
					});

					int size = outputList.size();

					if (size <= BATCH_LIMIT) {
						outputList.add(0, firstMap);
						publish(topic, outputList);
					} else {
						List<Map<String, Object>> subList = null;
						for (int i = 0; i < size; i += BATCH_LIMIT) {
							subList = outputList.stream().skip(i).limit(BATCH_LIMIT).map(y -> y)
									.collect(Collectors.toList());
							firstMap.put(mapKey.getTotal(), subList.size());
							subList.add(0, firstMap);
							publish(topic, subList);
						}
					}
				}
			} else {
				logger.info(outputList.toString());
				logger.info("Finish Last Step: {}", map.toString());
			}

		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DAILY_LIST, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDailyList(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DAILY_LIST, map.toString());

		try {
			final String directory = map.getOrDefault(mapKey.getDirectory(), "-").toString();

			List<String> files = fileParseService.getSymbolsfromDailyDirectory(directory);
			files.forEach(System.out::println);

			files.stream().parallel().forEach(x -> {
				final Map<String, Object> fileMap = new TreeMap<>();
				map.forEach((i, j) -> {
					fileMap.put(i, j);
				});
				fileMap.put(mapKey.getFileName(), x);
				parseDailyData(fileMap);
			});

		} catch (IOException e) {
			logger.info(e.toString());
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_DAILY_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDaily(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_DAILY_SINGLE, map.toString());

		parseDailyData(map);

	}

	private void parseDailyData(Map<String, Object> map) {
		try {
			String dataSource = map.getOrDefault(mapKey.getDataSource(), "-").toString();
			String directory = map.getOrDefault(mapKey.getDirectory(), "-").toString();
			String fileName = map.getOrDefault(mapKey.getFileName(), "-").toString();

			final String topic = getTopicFromList(map);

			List<Map<String, Object>> outputList = fileParseService.parseDailyFile(directory, fileName, dataSource);

			if (topic != null) {
				logger.info("Number of records:" + outputList.size());
				if (outputList.size() >= 2) {
					final Map<String, Object> firstMap = outputList.remove(0);
					map.forEach((i, j) -> {
						firstMap.put(i, j);
					});

					int size = outputList.size();

					if (size <= BATCH_LIMIT) {
						outputList.add(0, firstMap);
						publish(topic, outputList);
					} else {
						List<Map<String, Object>> subList = null;
						for (int i = 0; i < size; i += BATCH_LIMIT) {
							subList = outputList.stream().skip(i).limit(BATCH_LIMIT).map(y -> y)
									.collect(Collectors.toList());
							firstMap.put(mapKey.getTotal(), subList.size());
							subList.add(0, firstMap);
							publish(topic, subList);
						}
					}
				}
			} else {
				logger.info(outputList.toString());
				logger.info("Finish Last Step: {}", map.toString());
			}

		} catch (IOException e) {
			logger.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_PARSE_INFO_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void parseInfo(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_PARSE_INFO_SINGLE, map.toString());

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