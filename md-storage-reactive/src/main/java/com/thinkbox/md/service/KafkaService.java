package com.thinkbox.md.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

@Service
@Slf4j
public class KafkaService {

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;
	@Autowired
	private StoreService storeService;

	@Autowired
	private MapKeyParameter mapKey;

	@Value("${kafka.topic.save-dailysummary-list}")
	private String topicSaveDailysummaryList;

	@Value("${kafka.topic.save-tradedate-list}")
	private String topicSaveTradedateList;

	@Value("${kafka.topic.save-exchange-list}")
	private String topicSaveExchangeList;

	@Value("${kafka.topic.save-analysis-list}")
	private String topicSaveAnalysisList;

	@Value("${kafka.topic.save-historical-list}")
	private String topicSaveHistoricalList;

	@Value("${kafka.topic.save-instrument-list}")
	private String topicSaveInstrumentList;

	@Value("${kafka.topic.save-instrument-single}")
	private String topicSaveInstrumentSingle;

	@Value("${kafka.topic.dbget-exchange-data}")
	private String topicDBgetExchangeData;

	@Value("${kafka.topic.dbget-total-from-instrument}")
	private String topicDBgetTotalFromInstrument;

	@Value("${kafka.topic.dbget-summary-single}")
	private String topciDBgetSummarySingle;

	@Value("${kafka.topic.dbget-summary-list}")
	private String topicDBgetSummaryList;

	@Value("${kafka.topic.dbget-historical-single}")
	private String topicDBgetHistoricalSingle;

	@Value("${kafka.topic.dbget-historical-list}")
	private String topicDBgetHistoricalList;

	@Value("${kafka.topic.dbget-analysis-single}")
	private String topicDBgetAnalysisSingle;

	@Value("${kafka.topic.dbget-analysis-list}")
	private String topicDBgetAnalysisList;

	@Value("${kafka.topic.dbget-tradedate-single}")
	private String topicDBgetTradedateSingle;

	@Value("${kafka.topic.dbget-tradedate-list}")
	private String topicDBgetTradedateList;

	@Value("${kafka.topic.dbget-analysis-tradedate}")
	private String topicDBgetAnalysisTradedate;

	@Value("${kafka.topic.consolidate-historical-ticker}")
	private String topicConsolidateHistoricalTicker;

	private ConcurrentHashMap<String, AtomicInteger> counterMap;

	private ConcurrentHashMap<String, List<String>> fileMap;

//	@Value("${kafka.topic.dbget-dailysummary-single}")
//	private String TOPIC_DBGET_DAILYSUMMARY_SINGLE;
//
//	@Value("${kafka.topic.dbupdate-historical-all}")
//	private String TOPIC_DBUPDATE_HISTORICAL_ALL;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final static String ASYNC_EXECUTOR = "asyncExecutor";

	private final static String CONTAINER_FACTORY_LIST = "listListener";

	private final static String CONTAINER_FACTORY_MAP = "mapListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	private final static String OUTPUT_DATE_FORMAT = "%1$tY%1$tm%1$td";

	private final static String DATE_FORMAT = "yyyyMMdd";

	private final static String DEFAULT_STRING_VALUE = "-";

//	private final static Double DEFAULT_DOUBLE_VALUE = 0d;

	private final static String FILE_EXTENSION_JSON = ".json";

	private final static String TOPIC_DELIMITER = "[-]";

	private final static String DEFAULT_TOPIC_ACTION = "unknown";

	private final static String DEFAULT_TOPIC_TYPE = "unknown";

	private final static String OUTPUT_FORMAT_JSON = "JSON";

	private final static String STRING_PERIOD = ".";

	private final static String STRING_COMMA = ",";

	private final static String STRING_EMPTY_SPACE = "";

	private final static String STRING_SQUARE_OPEN_BRACKET = "[";

	private final static String STRING_SQUARE_CLOSE_BRACKET = "]";

	private final static String STRING_CURLY_BRACKET = "{}";

//	private final static int BATCH_LIMIT = 1500;

//	private final static int DEFAULT_LIMIT = 2;

	private final static int OBJECT_TYPE_INSTRUMENT = 1;

	private final static int OBJECT_TYPE_HISTORICAL = 2;

	private final static int OBJECT_TYPE_ANALYSIS = 3;

	private final static int OBJECT_TYPE_TRADEDATE = 4;

	private final static int OBJECT_TYPE_DAILYSUMMARY = 5;

	@PostConstruct
	private void init() {
		counterMap = new ConcurrentHashMap<>();
		fileMap = new ConcurrentHashMap<>();
		;
	}

	public void publish(String topic, Map<String, Object> map) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, map.toString());
		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, list.get(0).toString());

		kafkaTemplateList.send(topic, list);
	}

//	private void publish(String topic, Map<String, Object> map, List<Map<String, Object>> list) {
//		int size = list.size();
//
//		if (size <= BATCH_LIMIT) {
//			map.put(mapKey.getTotal(), size);
//			list.add(0, map);
//			publish(topic, list);
//		} else {
//			List<Map<String, Object>> subList = null;
//			for (int i = 0; i < size; i += BATCH_LIMIT) {
//				subList = list.stream().skip(i).limit(BATCH_LIMIT).map(y -> y).collect(Collectors.toList());
//				map.put(mapKey.getTotal(), subList.size());
//				subList.add(0, map);
//				publish(topic, subList);
//			}
//		}
//	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-exchange-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveExchangeList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveExchangeList, list.toString());
		storeService.saveMapList(OBJECT_TYPE_INSTRUMENT, list, () -> {
		});
	}

	private Map<String, Object> readFile(Map<String, Object> firstMap, String topic, String fileName) {

		final String requestID = firstMap.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		File file = getFile(requestID, topic, fileName);
		Map<String, Object> map = null;
		try (FileInputStream fileInputStream = new FileInputStream(file)) {

			map = objectMapper.readValue(fileInputStream, new TypeReference<Map<String, Object>>() {
			});

		} catch (IOException e) {
			log.info("Exception: " + e.toString());
			e.printStackTrace();
		}
		return map;
	}

	private Flux<Map<String, Object>> readFileListFlux(String fullFileName) {

		Path path = Paths.get(fullFileName);

		return Flux.using(() -> Files.lines(path), Flux::fromStream, BaseStream::close).filter(x -> x.length() >= 4)
				.map(x -> {
					String y = x.replace("[", STRING_EMPTY_SPACE).replace("]", STRING_EMPTY_SPACE);
					y = y.substring(0, y.length() - 1);
//			System.out.println("y:" + y);
					Map<String, Object> z = new TreeMap<>();
					try {
						z = objectMapper.readValue(y, new TypeReference<Map<String, Object>>() {
						});
					} catch (JsonProcessingException e) {
						e.printStackTrace();
						log.info("Y (can't parse):" + y);
					} catch (RuntimeException e) {
						e.printStackTrace();
						log.info("X:" + x.toString());
					}
					return z;

				}).filter(x -> x.containsKey(mapKey.getTicker()));
	}

	private List<Map<String, Object>> readFileList2(Map<String, Object> firstMap, String currentTopic,
			String fileName) {
		String[] topicBreakDown = currentTopic.split(TOPIC_DELIMITER);
		String topicAction = DEFAULT_TOPIC_ACTION;
		String topicType = DEFAULT_TOPIC_TYPE;
		if (topicBreakDown.length >= 2) {
			topicAction = topicBreakDown[0];
			topicType = topicBreakDown[1];
		}
		final String requestID = firstMap.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();

		File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator + topicType
				+ File.separator + requestID + STRING_PERIOD + fileName + FILE_EXTENSION_JSON);
		List<Map<String, Object>> mapperList = null;

		try (FileInputStream fileInputStream = new FileInputStream(file)) {

			mapperList = objectMapper.readValue(fileInputStream, new TypeReference<List<Map<String, Object>>>() {
			});
			mapperList.add(0, firstMap);

		} catch (IOException e) {
			log.info("Exception: " + e.toString());
			e.printStackTrace();
		}
		return mapperList;
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-dailysummary-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveDailySummaryList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveDailysummaryList, list.get(0).toString());

		final Map<String, Object> firstMap = list.get(0);
		final String date = firstMap.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String type = firstMap.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

		saveMap(firstMap, OBJECT_TYPE_DAILYSUMMARY, type, date);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-tradedate-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveTradeDateList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveTradedateList, list.get(0).toString());

		saveListFlux(list, OBJECT_TYPE_TRADEDATE, "tradedates");
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-instrument-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrumentList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveInstrumentList, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		saveListFlux(list, OBJECT_TYPE_INSTRUMENT, subExch);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-analysis-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveAnalysisList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveAnalysisList, list.toString());

		Map<String, Object> firstMap = list.get(0);
		final String ticker = firstMap.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		saveListFlux(list, OBJECT_TYPE_ANALYSIS, ticker);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-historical-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveHistoricalList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveHistoricalList, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);
		final String fileName = firstMap.getOrDefault(mapKey.getFileName(), DEFAULT_STRING_VALUE).toString();
		saveListFlux(list, OBJECT_TYPE_HISTORICAL, fileName);
	}

	private void saveListFlux(List<Map<String, Object>> list, int objType, String fileName) {
		final Map<String, Object> firstMap = list.get(0);
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);
		final String requestID = firstMap.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();

		final String currentFullFileName = getFullFileName(requestID, currentTopic, fileName);
		final String nextFullFileName = topic == null ? null : getFullFileName(requestID, topic, fileName);

		final Flux<Map<String, Object>> fluxMap = readFileListFlux(currentFullFileName);
		storeService.saveMapListFlux(objType, fluxMap, () -> {
			completeActionForSaveMapListFlux(firstMap, topic, currentFullFileName, nextFullFileName);
		});
	}

	private void completeActionForSaveMapListFlux(Map<String, Object> map, String topic, String currentFullFileName,
			String nextFullFileName) {
//		System.out.println("Current: " + currentFullFileName);
//		System.out.println("Next: " + nextFullFileName);
//		System.out.println("FirstMap: " + map);
//		System.out.println("Topic: " + topic);
		if (topic != null) {
			Path sourcePath = Paths.get(currentFullFileName);
			Path targetPath = Paths.get(nextFullFileName);

			try {
				Files.copy(sourcePath, targetPath);
				publishAfterOutputAsFile(map, topic, 0);
			} catch (IOException e) {
				log.info("Exception: " + e.toString());
//				e.printStackTrace();
			}

		} else {
			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
	}

	private void saveMap(Map<String, Object> firstMap, int objType, String type, final String fileName) {
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		final Map<String, Object> map = readFile(firstMap, currentTopic, fileName);

		storeService.saveMap(objType, type, map, () -> {
			completeActionForSaveMap(map, firstMap, topic, fileName);
		});
	}

	private void completeActionForSaveMap(Map<String, Object> map, Map<String, Object> firstMap, String topic,
			String fileName) {
		if (map != null) {
			if (topic != null) {
				List<Map<String, Object>> list = new ArrayList<>();
				list.add(map);
				outputAsFile(map, firstMap, topic, fileName);
			} else {
				log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			log.info("map is null");
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.save-instrument-single}", containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrument(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicSaveInstrumentSingle, list.get(0).toString());

		final Map<String, Object> secondMap = list.get(1);
		final Map<String, Object> firstMap = list.get(0);

		storeService.saveMap(OBJECT_TYPE_INSTRUMENT, null, secondMap, () -> {
			completeActionForSaveInstrument(firstMap, secondMap);
		});
	}

	private void completeActionForSaveInstrument(Map<String, Object> firstMap, Map<String, Object> secondMap) {
		String topic = getTopicFromList(firstMap);
		if (topic != null) {
			List<Map<String, Object>> outputList = new ArrayList<>();
			outputList.add(firstMap);
			outputList.add(secondMap);

			publish(topic, outputList);
		} else {
			log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
		}
	}

//	@Async(ASYNC_EXECUTOR)
//	@KafkaListener(topics = "${kafka.topic.dbget-total-from-instrument}", containerFactory = CONTAINER_FACTORY_MAP)
//	public void getHistoricalTotalFromInstruments(Map<String, Object> map) {
//		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetTotalFromInstrument, map.toString());
//
//		List<Map<String, Object>> outputList = null;
//
//		final Integer limit = Integer.valueOf(map.getOrDefault(mapKey.getLimit(), DEFAULT_LIMIT).toString());
//		final String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
//		final String topic = getTopicFromList(map);
//
//		if (!subExch.equals(DEFAULT_STRING_VALUE)) {
//			outputList = storeService.getHistoricalTotalFromInstrument(subExch, limit);
//		}
//
//		if (topic != null) {
//			if (outputList != null) {
//				publish(topic, map, outputList);
//			}
//		} else {
//			if (outputList != null) {
//				printList(outputList);
//			}
//			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
//		}
//	}

//	private void printList(List<Map<String, Object>> outputList) {
//		outputList.forEach(x -> {
//			log.info("Ticker: " + x.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString() + " - "
//					+ x.getOrDefault(mapKey.getHTotal(), 0).toString() + " -- "
//					+ x.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString());
//		});
//		log.info("Total: " + outputList.size());
//	}

	// TODO rewrite in reactive style
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-exchange-data}", containerFactory = CONTAINER_FACTORY_MAP)
	public void getInstruments(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetExchangeData, map.toString());

		Flux<Map<String, Object>> outputList = null;
		Flux<Map<String, Object>> outList = null;

		final String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		final String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		final String type = map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);

		int max = Integer.valueOf(map.getOrDefault("max", 0).toString());

		outputList = storeService.getInstrumentMapListFlux(subExch, type, ticker);

		if (max != 0) {
			outList = outputList.take(max);
		} else {
			outList = outputList;
		}

//		int size = outList.size();
//		if (size > 0) {
		if (topic != null) {
			processFlux(outList, map, topic, subExch);
		} else {
//				printList(outList);
			log.info("Size: " + outList.count().block());
			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
//		} else {
//			log.info("outList size is zero");
//		}
//		System.out.println(storeService.updateAnalysisField("RFP", "20220502", "ind.obv", 21));
//		System.out.println("50 > 200 Counter:" + storeService.countByCriterion("this.ind.sma50>this.ind.sma200"));
//		System.out.println("21 > 50 Counter:" + storeService.countByCriterion("this.ind.sma21>this.ind.sma50"));

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-analysis-tradedate}", containerFactory = CONTAINER_FACTORY_LIST)
	public void getAnalysisDate(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetAnalysisTradedate, map.toString());
		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);

		Flux<Map<String, Object>> outputList = null;

		outputList = storeService.getDateMapList(date);

		// TODO redesign to avoid counting
//		Mono<Long> count = outputList.count();
//		long size = count.block();
//		if (size > 0) {
		if (topic != null) {

			processFlux(outputList, map, topic, "tradedates");

		} else {
			log.info("Size: " + outputList.count().block());
			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
//		} else {
//			log.info("outputList size is zero");
//		}
	}

	// TODO rewrite in reactive style
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-tradedate-list}", containerFactory = CONTAINER_FACTORY_MAP)
	public void getTradeDateList(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetTradedateList, map.toString());

		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String type = map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

		List<Map<String, Object>> list = null;
		if (date.equals(DEFAULT_STRING_VALUE)) {
			list = storeService.getTradeDateList();
		} else {
			list = storeService.getTradeDateGreaterThanList(date);
		}

		if (list.size() > 0) {
			list.stream()
					.sorted((i, j) -> i.get(mapKey.getDate()).toString().compareTo(j.get(mapKey.getDate()).toString()))
					.forEach(x -> {
						String mDate = x.get(mapKey.getDate()).toString();

						Map<String, Object> newMap = map.entrySet().stream()
								.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

						newMap.put(mapKey.getDate(), mDate);

						getDataFlux(newMap, OBJECT_TYPE_TRADEDATE, type, mDate);
					});
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-tradedate-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void getTradeDateSingle(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetTradedateSingle, map.toString());

		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String type = map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

		List<Map<String, Object>> list = storeService.getTradeDateList(date);

		if (list.size() > 0) {
			getDataFlux(map, OBJECT_TYPE_TRADEDATE, type, date);
		}
	}

	// TODO rewrite in reactive style
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-analysis-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void getAnalysisList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetAnalysisList, list.get(0).toString());
		final Map<String, Object> firstMap = list.get(0);
		final String type = firstMap.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

		getDataList(list, OBJECT_TYPE_ANALYSIS, type);
	}

	// TODO rewrite in reactive style
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-historical-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetHistoricalList, list.get(0).toString());
		final Map<String, Object> firstMap = list.get(0);
		final String type = firstMap.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

		getDataList(list, OBJECT_TYPE_HISTORICAL, type);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-analysis-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void getAnalysis(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetAnalysisSingle, map.toString());

		final String ticker = map.get(mapKey.getTicker()).toString();
		Map<String, Object> instrument = storeService.getInstrument(ticker);

		if (instrument != null) {
			String type = instrument.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();

			getDataFlux(map, OBJECT_TYPE_ANALYSIS, type, ticker);
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-historical-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void getHistorical(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetHistoricalSingle, map.toString());

		final String ticker = map.get(mapKey.getTicker()).toString();

		getDataFlux(map, OBJECT_TYPE_HISTORICAL, null, ticker);

	}

	private void getDataFlux(Map<String, Object> map, int objType, String type, String criterion) {

		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final Integer day = Integer.parseInt(map.getOrDefault(mapKey.getDay(), 0).toString());
		final String topic = getTopicFromList(map);

		Flux<Map<String, Object>> flux = null;

		Calendar calendar = getCalendar(DATE_FORMAT, date);

		String formattedDate = null;
		if (calendar != null) {
			calendar.add(Calendar.DATE, -day);
			formattedDate = String.format(OUTPUT_DATE_FORMAT, calendar);
		} else {
			formattedDate = DEFAULT_STRING_VALUE;
		}

		if (objType == OBJECT_TYPE_HISTORICAL) {
			flux = storeService.getHistoricalMapFlux(criterion, formattedDate);
		} else if (objType == OBJECT_TYPE_ANALYSIS) {
			flux = storeService.getAnalysisMapFluxByTickerAndDate(type, criterion, formattedDate);
		} else if (objType == OBJECT_TYPE_TRADEDATE) {
			flux = storeService.getAnalysisMapFluxByTickerAndDate(type, DEFAULT_STRING_VALUE, criterion);
		} else {

		}

//		System.out.println("Criterion: " + criterion);		
//		Mono<Long> count = outputList.count();
//		long size = count.block();
//		System.out.println("Size: " + size);
//		if (size > 0) {
		if (topic != null) {
			processFlux(flux, map, topic, criterion);
		} else {
			log.info("Flux Size: " + flux.count().block());
			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
//		} else {
//			log.info("outputList size is zero");
//		}
	}

	private void processFlux(Flux<Map<String, Object>> flux, final Map<String, Object> map, final String topic,
			final String fileName) {

		final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		final String outFileName = getFullFileName(requestID, topic, fileName);
		final String dataFormat = map.getOrDefault(mapKey.getDataFormat(), DEFAULT_STRING_VALUE).toString();

		final Path outPath = Paths.get(outFileName);
		System.out.println("OutPath: " + outPath.toString());
		try {
			BufferedWriter bw;
			bw = Files.newBufferedWriter(outPath, StandardOpenOption.CREATE);
			if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
				bw.write(STRING_SQUARE_OPEN_BRACKET);
			}
			flux.subscribe(s -> writeOutputToFile(dataFormat, bw, s), (e) -> closeOutputToFile(bw),
					() -> completeOutputToFile(flux, dataFormat, bw, map, topic));

		} catch (IOException e) {
			log.info("Exception when processing Flux: " + e.toString());
			e.printStackTrace();
		}
	}

	private void closeOutputToFile(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void completeOutputToFile(Flux<Map<String, Object>> flux, String dataFormat, BufferedWriter bw,
			Map<String, Object> map, String topic) {
		try {
			if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
				bw.write(STRING_CURLY_BRACKET + STRING_SQUARE_CLOSE_BRACKET);
			}
			bw.close();
			if (topic != null) {
				publishAfterOutputAsFile(map, topic, 0);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void writeOutputToFile(String dataFormat, BufferedWriter bw, Map<String, Object> map) {
		try {
			bw.write(objectMapper.writeValueAsString(map)
					+ (dataFormat.equals(OUTPUT_FORMAT_JSON) ? STRING_COMMA : STRING_EMPTY_SPACE));
			bw.newLine();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void getDataList(List<Map<String, Object>> list, final int objType, final String type) {
		final Map<String, Object> firstMap = list.get(0);

		final String currentTopic = getCurrentTopicFromList(firstMap);

		final String fileName = firstMap.getOrDefault(mapKey.getFileName(), DEFAULT_STRING_VALUE).toString();
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		final String requestID = firstMap.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();

		final String currentFullFileName = getFullFileName(requestID, currentTopic, !fileName.equals(DEFAULT_STRING_VALUE)?fileName:subExch);

		System.out.println("currentFullFileName:" + currentFullFileName);
		Flux<Map<String, Object>> fluxMap = readFileListFlux(currentFullFileName);

		fluxMap.map(x -> {
			Map<String, Object> newMap = firstMap.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			newMap.put(mapKey.getTicker(), x.get(mapKey.getTicker()).toString());
			return newMap;
		}).subscribe(y -> {
			getDataFlux(y, objType, type, y.get(mapKey.getTicker()).toString());
		}, (e) -> {
		}, () -> {
		});
	}

	private void outputAsFile(Map<String, Object> outMap, Map<String, Object> map, String topic, String fileName) {
		try {
			final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
			File file = getFile(requestID, topic, fileName);

			objectMapper.writeValue(file, outMap);

			publishAfterOutputAsFile(map, file, topic, 1);
		} catch (IOException e) {
			log.info("Exception: " + e.toString());
			e.printStackTrace();
		}
	}

	private void outputAsFileList(List<Map<String, Object>> outputList, Map<String, Object> map, String topic,
			String fileName) {
		try {
			final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
			File file = getFile(requestID, topic, fileName);

			objectMapper.writeValue(file, outputList);

			publishAfterOutputAsFile(map, file, topic, outputList.size());
		} catch (IOException e) {
			log.info("Exception: " + e.toString());
			e.printStackTrace();
		}
	}

	private File getFile(String requestID, String topic, String fileName) {

		return new File(getFullFileName(requestID, topic, fileName));

	}

	private String getFullFileName(String requestID, String topic, String fileName) {
		String[] topicBreakDown = topic.split(TOPIC_DELIMITER);
		String topicAction = DEFAULT_TOPIC_ACTION;
		String topicType = DEFAULT_TOPIC_TYPE;
		if (topicBreakDown.length >= 2) {
			topicAction = topicBreakDown[0];
			topicType = topicBreakDown[1];
		}

		return System.getProperty(USER_HOME) + File.separator + topicAction + File.separator + topicType
				+ File.separator + requestID + STRING_PERIOD + fileName + FILE_EXTENSION_JSON;
	}

	private void publishAfterOutputAsFile(Map<String, Object> map, String topic, long size) {
		publishAfterOutputAsFile(map, null, topic, size);
	}

	private void publishAfterOutputAsFile(Map<String, Object> map, File file, String topic, long size) {
		Map<String, Object> newMap = map.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		if (size != 0)
			newMap.put(mapKey.getTotal(), size);

		if (file != null) {
			newMap.put(mapKey.getLength(), file.length());
		}
		List<Map<String, Object>> outList = new ArrayList<>();

		outList.add(0, newMap);
//		System.out.println("Before publish");
		publish(topic, outList);
	}

	private Calendar getCalendar(String dateFormat, String date) {
		Calendar calendar = null;
		Date sDate;
		try {
			sDate = new SimpleDateFormat(dateFormat).parse(date);
			calendar = Calendar.getInstance();
			calendar.setTime(sDate);
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.HOUR, 0);
		} catch (ParseException e) {
			log.debug(e.toString());
		}
		return calendar;
	}

	// TODO rewrite in reactive style
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-summary-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummaryList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicDBgetSummaryList, list.get(0).toString());

		final Map<String, Object> firstMap = list.get(0);
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

		list = readFileList2(firstMap, currentTopic, subExch);

		Instant start = Instant.now();

		list.remove(0);
		list.stream().parallel().forEach(x -> {
			updateSummary(x);
		});

		log.info("Total time for getting summary:" + logTime(start));
		if (list.size() > 0) {
			if (topic != null) {
				outputAsFileList(list, firstMap, topic, subExch);
			} else {
				log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			log.info("outputList size is zero");
		}

	}

	private void updateSummary(Map<String, Object> x) {
		try {
			String ticker = x.get(mapKey.getTicker()).toString();

			Map<String, Object> summaryMap = storeService.getHistoricalSummary(ticker);

			x.put(mapKey.getHTotal(), summaryMap.getOrDefault(mapKey.getHTotal(), 0));
			x.put(mapKey.getHFirstD(), summaryMap.getOrDefault(mapKey.getHFirstD(), DEFAULT_STRING_VALUE));
			x.put(mapKey.getHLastD(), summaryMap.getOrDefault(mapKey.getHLastD(), DEFAULT_STRING_VALUE));

			Double last = Double.valueOf(summaryMap.getOrDefault(mapKey.getLastP(), 0).toString());
			if (last != 0) {
				x.put(mapKey.getLastP(), last);
				Long sharesO = Long.valueOf(x.getOrDefault(mapKey.getSharesO(), 0).toString());
				if (sharesO != 0) {
					x.put(mapKey.getMCap(), Double.valueOf(last * sharesO).longValue());
				}
			}

			Double hHigh = Double.valueOf(summaryMap.getOrDefault(mapKey.getHHigh(), 0).toString());
			x.put(mapKey.getHHigh(), hHigh);
			if (hHigh != 0) {
				String hHighD = storeService.getHistoricalDate(ticker, hHigh);
				x.put(mapKey.getHHighD(), hHighD);
			}

			Double hLow = Double.valueOf(summaryMap.getOrDefault(mapKey.getHLow(), 0).toString());
			x.put(mapKey.getHLow(), hLow);
			if (hLow != 0) {
				String hLowD = storeService.getHistoricalDate(ticker, hLow);
				x.put(mapKey.getHLowD(), hLowD);
			}
		} catch (Exception e) {
			System.out.println("X:" + x.toString());
			e.printStackTrace();
		}
	}

//	private void updateSummaryFromAllRecords(Map<String, Object> x) {
//
//		String ticker = x.get(mapKey.getTicker()).toString();
//
//		Map<String, Object> y = storeService.getHistoricalSummaryFromAllRecords(ticker);
//
//		y.entrySet().forEach((i) -> {
//			x.put(i.getKey(), i.getValue());
//		});
//
//		Double last = Double.valueOf(y.getOrDefault(mapKey.getLastP(), DEFAULT_DOUBLE_VALUE).toString());
//		if (last != 0) {
//			Long sharesO = Long.valueOf(x.getOrDefault(mapKey.getSharesO(), 0).toString());
//			if (sharesO != 0) {
//				x.put(mapKey.getMCap(), Double.valueOf(last * sharesO).longValue());
//			}
//		}
//
//	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.dbget-summary-single}", containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummary(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topciDBgetSummarySingle, map.toString());

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
			log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.consolidate-historical-ticker}", containerFactory = CONTAINER_FACTORY_LIST)
	public void consolidateHistoricalList(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicConsolidateHistoricalTicker, map.toString());

		final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		final Integer files = (Integer) map.getOrDefault(mapKey.getFiles(), 0);
		final String fileName = map.getOrDefault(mapKey.getFileName(), DEFAULT_STRING_VALUE).toString();
		final String dataFormat = map.getOrDefault(mapKey.getDataFormat(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(map);
		
		final String topic = getTopicFromList(map);
		AtomicInteger atomicInteger = counterMap.get(requestID);
		List<String> fileList = fileMap.get(requestID);
		if (atomicInteger == null) {
			atomicInteger = new AtomicInteger();
			counterMap.put(requestID, atomicInteger);
			fileList = new ArrayList<>();
			fileMap.put(requestID, fileList);
		}
		fileList.add(getFullFileName(requestID, currentTopic, fileName));
		if (atomicInteger.incrementAndGet() == files) {
//			System.out.println("Done: " + atomicInteger.get() + " - " + files);
//			System.out.println("Files path: " + fileList.toString());

			final String outFileName = getFullFileName(requestID, topic, "consolidate");
			final Path outPath = Paths.get(outFileName);

			map.put(mapKey.getFileName(), "consolidate");
			List<Flux<String>> fluxList = new ArrayList<>();
			fileList.parallelStream().forEach(x -> {
				fluxList.add(getTickerFromFiles(x));
			});
			try {
				BufferedWriter bw;
				bw = Files.newBufferedWriter(outPath, StandardOpenOption.CREATE);
				if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
					bw.write(STRING_SQUARE_OPEN_BRACKET);
				}

				Flux<Map<String, Object>> flux = Flux.merge(fluxList).distinct().map(x -> {
					Map<String, Object> m = Map.ofEntries(Map.entry(mapKey.getTicker(), x));
					return m;
				});
				flux.subscribe(s -> writeOutputToFile(dataFormat, bw, s), (e) -> closeOutputToFile(bw),
						() -> completeOutputToFile(flux, dataFormat, bw, map, topic));
			} catch (IOException e) {
				log.info("Exception when processing Flux: " + e.toString());
				e.printStackTrace();
			}
//			System.out.println("Count: " + fluxString.count().block());
//		} else {
//			System.out.println("++: " + atomicInteger.get() + " - " + files);
		}
	}

	private Flux<String> getTickerFromFiles(String fullFileName) {
		return readFileListFlux(fullFileName).map(x -> x.get("ticker").toString());
	}

//	@Async(ASYNC_EXECUTOR)
//	@KafkaListener(topics = TOPIC_DBUPDATE_HISTORICAL_ALL, containerFactory = CONTAINER_FACTORY_MAP)
//	public void updateHistoricalAll(Map<String, Object> map) {
//		log.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBUPDATE_HISTORICAL_ALL, map.toString());
//
//		final String topic = getTopicFromList(map);
//
//		Integer loop = Integer.valueOf(map.get("loop").toString());
//		IntStream.range(0, loop).parallel().forEach(k -> {
//			List<Map<String, Object>> outputList = storeService.getHistoricalList(k);
//
//			System.out.println("Total:" + outputList.size());
//			if (topic != null) {
//
//				Map<String, Object> newMap = map.entrySet().stream()
//						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//				outputList.add(0, newMap);
//
//				publish(topic, outputList);
//			} else {
//				log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
//			}
//
//		});
//
//	}

	private String getCurrentTopicFromList(Map<String, Object> map) {
		Object objNext = map.get(mapKey.getNext());
		int next = Integer.valueOf(objNext.toString());

		Object objStep = map.get(mapKey.getSteps());

		@SuppressWarnings("unchecked")
		List<String> stepList = (List<String>) objStep;

		String topic = null;

		if (stepList.size() > next) {
			topic = stepList.get(next);
		}
		return topic;
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

	private String logTime(Instant start) {
		return Duration.between(start, Instant.now()).toMillis() + "ms";
	}
}