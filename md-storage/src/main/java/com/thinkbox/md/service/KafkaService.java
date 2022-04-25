package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.config.MapKeyParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

	private final String TOPIC_SAVE_INSTRUMENT_LIST = "save.instrument.list";

	private final String TOPIC_SAVE_DETAIL_SINGLE = "save.detail.single";

	private final String TOPIC_DBGET_EXCHANGE_DATA = "dbget.exchange.data";

	private final String TOPIC_DBGET_TOTAL_FROM_INSTRUMENT = "dbget.total.from.instrument";

	private final String TOPIC_DBGET_HISTORICAL_LIST = "dbget.historical.list";

	private final String TOPIC_DBUPDATE_HISTORICAL_TOTAL = "dbupdate.historical.total";

	private final String TOPIC_DBUPDATE_HISTORICAL_ALL = "dbupdate.historical.all";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final int BATCH_LIMIT = 2000;

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

		storeService.saveHistoricalList(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);
		if (topic != null) {
			publish(topic, list);
		} else {
			logger.info("Finish Last Step: {} {}", firstMap.toString());
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_INSTRUMENT_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrumentList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_SAVE_INSTRUMENT_LIST, list.toString());

		storeService.saveInstrumentList(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);
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
	@KafkaListener(topics = TOPIC_DBGET_TOTAL_FROM_INSTRUMENT, containerFactory = CONTAINER_FACTORY_MAP)
	public void getHistoricalTotalFromInstruments(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_TOTAL_FROM_INSTRUMENT, map.toString());

		List<Map<String, Object>> outputList = null;

		Integer limit = Integer.valueOf(map.getOrDefault("limit", "2").toString());
		String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();
		if (!subExchange.equals("-")) {
			outputList = storeService.getHistoricalTotalFromInstrument(subExchange, limit);
		}

//		outputList.forEach(x -> {
//			System.out.println(x.toString());
//		});

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {

				int size = outputList.size();

				if (size <= BATCH_LIMIT) {
					map.put(mapKey.getTotal(), size);
					outputList.add(0, map);
					publish(topic, outputList);
				} else {
					List<Map<String, Object>> subList = null;
					for (int i = 0; i < size; i += BATCH_LIMIT) {
						subList = outputList.stream().skip(i).limit(BATCH_LIMIT).map(y -> y)
								.collect(Collectors.toList());
						map.put(mapKey.getTotal(), subList.size());
						subList.add(0, map);
						publish(topic, subList);
					}
				}
			}
		} else {
			if (outputList != null) {
				
				outputList.forEach(x -> {
//					System.out.println(x.toString());
					System.out.println("Symbol: " + x.getOrDefault(mapKey.getSymbol(), "-").toString() + " - "
							+ x.getOrDefault(mapKey.getHistoricalTotal(), "-").toString());
				});
				System.out.println("Total: " + outputList.size());
				
			}
			logger.info("Finish Last Step: {}", map.toString());
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

//		outputList.forEach(x -> {
//			System.out.println(x.toString());
//		});

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {

				int size = outputList.size();

				if (size <= BATCH_LIMIT) {
					map.put(mapKey.getTotal(), size);
					outputList.add(0, map);
					publish(topic, outputList);
				} else {
					List<Map<String, Object>> subList = null;
					for (int i = 0; i < size; i += BATCH_LIMIT) {
						subList = outputList.stream().skip(i).limit(BATCH_LIMIT).map(y -> y)
								.collect(Collectors.toList());
						map.put(mapKey.getTotal(), subList.size());
						subList.add(0, map);
						publish(topic, subList);
					}
				}
			}
		} else {
			if (outputList != null) {
				
				outputList.forEach(x -> {
//					System.out.println(x.toString());
					System.out.println("Symbol: " + x.getOrDefault(mapKey.getSymbol(), "-").toString() + " - "
							+ x.getOrDefault(mapKey.getHistoricalTotal(), "-").toString());
				});
				System.out.println("Total: " + outputList.size());
				
			}
			logger.info("Finish Last Step: {}", map.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistorical(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_HISTORICAL_LIST, list.toString());

		final Map<String, Object> firstMap = list.get(0);
		final String topic = getTopicFromList(firstMap);

		list.stream().parallel().skip(1).forEach(x -> {

			String ticker = x.get(mapKey.getTicker()).toString();
			System.out.println("ticker: " + ticker);

			Map<String, Object> newMap = firstMap.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			newMap.put(mapKey.getTicker(), ticker);

			List<Map<String, Object>> outputList = storeService.getHistoricals(ticker, BATCH_LIMIT);
			System.out.println("ticker: " + ticker + " - size: " + outputList.size());

			if (topic != null) {
				int size = outputList.size();
				if (size > 0) {
					newMap.put(mapKey.getTotal(), size);
					outputList.add(0, newMap);
					outputList.forEach(System.out::println);

					// publish(topic, outputList);
				}
			} else {
				logger.info("Finish Last Step: {}", firstMap.toString());
			}
		});
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBUPDATE_HISTORICAL_ALL, containerFactory = CONTAINER_FACTORY_MAP)
	public void updateHistoricalAll(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBUPDATE_HISTORICAL_ALL, map.toString());

		final String topic = getTopicFromList(map);

		Integer loop = Integer.valueOf(map.get("loop").toString());
		IntStream.range(0, loop).parallel().forEach(k -> {
			List<Map<String, Object>> outputList = storeService.getHistoricals(k);
//			outputList.forEach(x -> {
//				System.out.println(x.get(mapKey.getSymbol()));
//			});
			System.out.println("Hello:" + outputList.size());
			if (topic != null) {

				Map<String, Object> newMap = map.entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

				outputList.add(0, newMap);

				publish(topic, outputList);
			} else {
				logger.info("Finish Last Step: {}", map.toString());
			}

		});

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBUPDATE_HISTORICAL_TOTAL, containerFactory = CONTAINER_FACTORY_LIST)
	public void updateHistoricalTotal(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBUPDATE_HISTORICAL_TOTAL, list.toString());

		Map<String, Object> firstMap = list.get(0);
		final String topic = getTopicFromList(firstMap);

		list.stream().parallel().skip(1).forEach(x -> {

			String symbol = x.get(mapKey.getSymbol()).toString();
			System.out.println("symbol: " + symbol);

			Long total = storeService.getHistoricalsTotal(symbol);

			x.put(mapKey.getHistoricalTotal(), total);
		});

		list.forEach(System.out::println);
		if (topic != null) {
			publish(topic, list);
		} else {
			logger.info("Finish Last Step: {}", firstMap.toString());
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