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

	private final static String ASYNC_EXECUTOR = "asyncExecutor";

	private final static String TOPIC_SAVE_EXCHANGE_LIST = "save.exchange.list";

	private final static String TOPIC_SAVE_HISTORICAL_LIST = "save.historical.list";

	private final static String TOPIC_SAVE_INSTRUMENT_LIST = "save.instrument.list";

	private final static String TOPIC_SAVE_INSTRUMENT_SINGLE = "save.instrument.single";

	private final static String TOPIC_DBGET_EXCHANGE_DATA = "dbget.exchange.data";

	private final static String TOPIC_DBGET_TOTAL_FROM_INSTRUMENT = "dbget.total.from.instrument";

	private final static String TOPIC_DBGET_SUMMARY_SINGLE = "dbget.summary.single";

	private final static String TOPIC_DBGET_SUMMARY_LIST = "dbget.summary.list";

	private final static String TOPIC_DBGET_HISTORICAL_LIST = "dbget.historical.list";

	private final static String TOPIC_DBUPDATE_HISTORICAL_ALL = "dbupdate.historical.all";

	private final static String CONTAINER_FACTORY_LIST = "listListener";

	private final static String CONTAINER_FACTORY_MAP = "mapListener";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String STRING_DASH = "-"; 
	
	private final int BATCH_LIMIT = 1500;

	public void publish(String topic, Map<String, Object> map) {
		logger.info(String.format("Sent topic: -> {}", map.toString()));
		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		logger.info("Sent topic: {} -> {}", topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	private void publish(String topic, Map<String, Object> map, List<Map<String, Object>> list) {
		int size = list.size();

		if (size <= BATCH_LIMIT) {
			map.put(mapKey.getTotal(), size);
			list.add(0, map);
			publish(topic, list);
		} else {
			List<Map<String, Object>> subList = null;
			for (int i = 0; i < size; i += BATCH_LIMIT) {
				subList = list.stream().skip(i).limit(BATCH_LIMIT).map(y -> y).collect(Collectors.toList());
				map.put(mapKey.getTotal(), subList.size());
				subList.add(0, map);
				publish(topic, subList);
			}
		}
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
			publish(topic, list.remove(0), list);
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
			publish(topic, list.remove(0), list);
		} else {
			logger.info("Finish Last Step: {} {}", firstMap.toString());
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_INSTRUMENT_SINGLE, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrument(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_SAVE_INSTRUMENT_SINGLE, list.toString());

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
		String subExchange = map.getOrDefault(mapKey.getSubExchange(), DEFAULT_STRING_VALUE).toString();
		if (!subExchange.equals(STRING_DASH)) {
			outputList = storeService.getHistoricalTotalFromInstrument(subExchange, limit);
		}

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {
				publish(topic, map, outputList);
			}
		} else {
			if (outputList != null) {

				outputList.forEach(x -> {
					System.out.println("Ticker: " + x.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString() + " - "
							+ x.getOrDefault(mapKey.getHistoricalTotal(), 0).toString());
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

		String subExchange = map.getOrDefault(mapKey.getSubExchange(), DEFAULT_STRING_VALUE).toString();
		if (!subExchange.equals("-")) {
			outputList = storeService.getInstruments(subExchange);
		}

		String topic = getTopicFromList(map);
		if (topic != null) {
			if (outputList != null) {
				publish(topic, map, outputList);
			}
		} else {
			if (outputList != null) {

				outputList.forEach(x -> {
					System.out.println("Ticker: " + x.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString() + " - "
							+ x.getOrDefault(mapKey.getHistoricalTotal(), 0).toString());
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

		list.stream().parallel().skip(1).limit(1).forEach(x -> {

			String ticker = x.get(mapKey.getTicker()).toString();
//			System.out.println("ticker: " + ticker);

			Map<String, Object> newMap = firstMap.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			newMap.put(mapKey.getTicker(), ticker);
			newMap.put("inst", x);

			List<Map<String, Object>> outputList = storeService.getHistoricals(ticker, BATCH_LIMIT);
			System.out.println("ticker: " + ticker + " - size: " + outputList.size());

			if (topic != null) {
				int size = outputList.size();
				if (size > 0) {
					newMap.put(mapKey.getTotal(), size);
					publish(topic, newMap, outputList);
				}
			} else {
				logger.info("Finish Last Step: {}", firstMap.toString());
			}
		});
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_SUMMARY_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummaryList(List<Map<String, Object>> list) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_SUMMARY_LIST, list.toString());

		final Map<String, Object> firstMap = list.get(0);
		final String topic = getTopicFromList(firstMap);

//		final String ticker = "RCFA";
		list.stream().parallel().skip(1).forEach(x -> {

			String ticker = x.get(mapKey.getTicker()).toString();

			Map<String, Object> summaryMap = storeService.getHistoricalSummary(ticker);

			x.put(mapKey.getHistoricalTotal(), summaryMap.getOrDefault(mapKey.getHistoricalTotal(), 0));
			x.put(mapKey.getHistoricalFirstDate(), summaryMap.getOrDefault(mapKey.getHistoricalFirstDate(), DEFAULT_STRING_VALUE));
			x.put(mapKey.getHistoricalLastDate(), summaryMap.getOrDefault(mapKey.getHistoricalLastDate(), DEFAULT_STRING_VALUE));
			x.put(mapKey.getLastPrice(), summaryMap.getOrDefault(mapKey.getLastPrice(), 0));

			String temp1 = summaryMap.getOrDefault(mapKey.getHistoricalHigh(), "?").toString();
			String[] temps1 = temp1.split(STRING_DASH);
			if (temps1.length == 2) {
				x.put(mapKey.getHistoricalHigh(), Double.valueOf(temps1[0]));
				x.put(mapKey.getHistoricalHighDate(), temps1[1]);				
			}
			String temp2 = summaryMap.getOrDefault(mapKey.getHistoricalLow(), "?").toString();
			String[] temps2 = temp2.split(STRING_DASH);
			if (temps2.length == 2) {
				x.put(mapKey.getHistoricalLow(), Double.valueOf(temps2[0]));
				x.put(mapKey.getHistoricalLowDate(), temps2[1]);				
			}
//			summaryMap.forEach((i, j) -> {
//				x.put(i, j);
//				System.out.println("Key:" + i);
//				System.out.println("Value:" + j);
//			});			
		});
		list.forEach(System.out::println);

		if (topic != null) {
			publish(topic, list.remove(0), list);
		} else {
			logger.info("Finish Last Step: {}", firstMap.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_SUMMARY_SINGLE, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummary(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBGET_SUMMARY_SINGLE, map.toString());

		final String topic = getTopicFromList(map);

		final String ticker = map.get(mapKey.getTicker()).toString();

		Map<String, Object> instrumentMap = storeService.getInstrument(ticker);

		Map<String, Object> summaryMap = storeService.getHistoricalSummary(ticker);

		summaryMap.forEach((i, j) -> {
			instrumentMap.put(i, j);
		});

		if (topic != null) {
			List<Map<String, Object>> outputList = new ArrayList<>();
			outputList.add(0, map);
			outputList.add(1, instrumentMap);

			publish(topic, outputList);
		} else {
			System.out.println(instrumentMap.toString());
			logger.info("Finish Last Step: {}", map.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBUPDATE_HISTORICAL_ALL, containerFactory = CONTAINER_FACTORY_MAP)
	public void updateHistoricalAll(Map<String, Object> map) {
		logger.info("Received topic: {} -> parameter: {}", TOPIC_DBUPDATE_HISTORICAL_ALL, map.toString());

		final String topic = getTopicFromList(map);

		Integer loop = Integer.valueOf(map.get("loop").toString());
		IntStream.range(0, loop).parallel().forEach(k -> {
			List<Map<String, Object>> outputList = storeService.getHistoricals(k);

			System.out.println("Total:" + outputList.size());
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