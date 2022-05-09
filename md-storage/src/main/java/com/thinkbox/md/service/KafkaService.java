package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.config.AnsiOutputApplicationListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final static String ASYNC_EXECUTOR = "asyncExecutor";

	private final static String TOPIC_SAVE_TRADEDATE_LIST = "save.tradedate.list";

	private final static String TOPIC_SAVE_EXCHANGE_LIST = "save.exchange.list";

	private final static String TOPIC_SAVE_ANALYSIS_LIST = "save.analysis.list";

	private final static String TOPIC_SAVE_HISTORICAL_LIST = "save.historical.list";

	private final static String TOPIC_SAVE_INSTRUMENT_LIST = "save.instrument.list";

	private final static String TOPIC_SAVE_INSTRUMENT_SINGLE = "save.instrument.single";

	private final static String TOPIC_DBGET_EXCHANGE_DATA = "dbget.exchange.data";

	private final static String TOPIC_DBGET_TOTAL_FROM_INSTRUMENT = "dbget.total.from.instrument";

	private final static String TOPIC_DBGET_SUMMARY_SINGLE = "dbget.summary.single";

	private final static String TOPIC_DBGET_SUMMARY_LIST = "dbget.summary.list";

	private final static String TOPIC_DBGET_HISTORICAL_SINGLE = "dbget.historical.single";

	private final static String TOPIC_DBGET_HISTORICAL_LIST = "dbget.historical.list";

	private final static String TOPIC_DBGET_ANALYSIS_SINGLE = "dbget.analysis.single";

	private final static String TOPIC_DBGET_ANALYSIS_LIST = "dbget.analysis.list";

	private final static String TOPIC_DBGET_TRADEDATE_SINGLE = "dbget.tradedate.single";

	private final static String TOPIC_DBGET_ANALYSIS_TRADEDATE = "dbget.analysis.tradedate";

	private final static String TOPIC_DBUPDATE_HISTORICAL_ALL = "dbupdate.historical.all";

	private final static String CONTAINER_FACTORY_LIST = "listListener";

	private final static String CONTAINER_FACTORY_MAP = "mapListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	private final static String OUTPUT_DATE_FORMAT = "%1$tY%1$tm%1$td";

	private final static String DATE_FORMAT = "yyyyMMdd";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static Double DEFAULT_DOUBLE_VALUE = 0d;

	private final static String FILE_EXTENSION = ".json";

	private final static String TOPIC_DELIMITER = "[.]";

	private final static String DEFAULT_TOPIC_ACTION = "unknown";

	private final static String DEFAULT_TOPIC_TYPE = "unknown";

	private final static int BATCH_LIMIT = 1500;

	private final static int DEFAULT_LIMIT = 2;

	private final static int OBJECT_TYPE_INSTRUMENT = 1;

	private final static int OBJECT_TYPE_HISTORICAL = 2;

	private final static int OBJECT_TYPE_ANALYSIS = 3;
	
	private final static int OBJECT_TYPE_TRADEDATE = 4;

	public void publish(String topic, Map<String, Object> map) {
//		logger.debug(STRING_LOGGER_SENT_MESSAGE, topic, map.toString());
		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
//		logger.debug(STRING_LOGGER_SENT_MESSAGE, topic, list.get(0).toString());

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
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_EXCHANGE_LIST, list.toString());
		storeService.saveInstrumentList(list);
	}

	private List<Map<String, Object>> readFile(Map<String, Object> firstMap, String currentTopic, String fileName) {
		String[] topicBreakDown = currentTopic.split(TOPIC_DELIMITER);
		String topicAction = DEFAULT_TOPIC_ACTION;
		String topicType = DEFAULT_TOPIC_TYPE;
		if (topicBreakDown.length >= 2) {
			topicAction = topicBreakDown[0];
			topicType = topicBreakDown[1];
		}
		File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator + topicType
				+ File.separator + fileName + FILE_EXTENSION);
		List<Map<String, Object>> mapperList = null;
		try {
			mapperList = objectMapper.readValue(new FileInputStream(file),
					new TypeReference<List<Map<String, Object>>>() {
					});

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapperList;
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_TRADEDATE_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveTradeDateList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_TRADEDATE_LIST, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);
		saveList(list, OBJECT_TYPE_TRADEDATE, "tradedates");
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_INSTRUMENT_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrumentList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_INSTRUMENT_LIST, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		saveList(list, OBJECT_TYPE_INSTRUMENT, subExch);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_ANALYSIS_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveAnalysisList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_ANALYSIS_LIST, list.toString());

		Map<String, Object> firstMap = list.get(0);
		final String ticker = firstMap.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		saveList(list, OBJECT_TYPE_ANALYSIS, ticker);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveHistoricalList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_HISTORICAL_LIST, list.get(0).toString());

		storeService.saveHistoricalList(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);
		if (topic != null) {
			publish(topic, list.remove(0), list);
		} else {
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
		}
	}

	private void saveList(List<Map<String, Object>> list, int objType, String fileName) {
		Map<String, Object> firstMap = list.get(0);
		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readFile(firstMap, currentTopic, fileName);
			list.add(0, firstMap);
		}
		if (objType == OBJECT_TYPE_INSTRUMENT) {
			storeService.saveInstrumentList(list);
		} else if (objType == OBJECT_TYPE_TRADEDATE){
			storeService.saveTradeDateList(list);
		} else {
			storeService.saveAnalysisList(list);
		}
		if (list.size() > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					publish(topic, list.remove(0), list);
				} else {
					outputAsFile(list, firstMap, topic, fileName);
				}
			} else {
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			logger.info("outputList size is zero");
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_SAVE_INSTRUMENT_SINGLE, containerFactory = CONTAINER_FACTORY_LIST)
	public void saveInstrument(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_SAVE_INSTRUMENT_SINGLE, list.get(0).toString());

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
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_TOTAL_FROM_INSTRUMENT, containerFactory = CONTAINER_FACTORY_MAP)
	public void getHistoricalTotalFromInstruments(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_TOTAL_FROM_INSTRUMENT, map.toString());

		List<Map<String, Object>> outputList = null;

		final Integer limit = Integer.valueOf(map.getOrDefault(mapKey.getLimit(), DEFAULT_LIMIT).toString());
		final String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);

		if (!subExch.equals(DEFAULT_STRING_VALUE)) {
			outputList = storeService.getHistoricalTotalFromInstrument(subExch, limit);
		}

		if (topic != null) {
			if (outputList != null) {
				publish(topic, map, outputList);
			}
		} else {
			if (outputList != null) {
				printList(outputList);
			}
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
	}

	private void printList(List<Map<String, Object>> outputList) {
		outputList.forEach(x -> {
			logger.info("Ticker: " + x.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString() + " - "
					+ x.getOrDefault(mapKey.getHTotal(), 0).toString() + " -- "
					+ x.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString());
		});
		logger.info("Total: " + outputList.size());
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_EXCHANGE_DATA, containerFactory = CONTAINER_FACTORY_MAP)
	public void getInstruments(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_EXCHANGE_DATA, map.toString());

		List<Map<String, Object>> outputList = null;
		List<Map<String, Object>> outList = null;

		final String format = map.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		final String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		final String type = map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);

		int max = Integer.valueOf(map.getOrDefault("max", 0).toString());

		if (!subExch.equals(DEFAULT_STRING_VALUE)) {
			if (!type.equals(DEFAULT_STRING_VALUE)) {
				if (!ticker.equals(DEFAULT_STRING_VALUE)) {
					outputList = storeService.getInstrumentList(subExch, type, ticker);
				} else {
					outputList = storeService.getInstrumentList(subExch, type);
				}
			} else {
				outputList = storeService.getInstrumentList(subExch);
			}
		}
		logger.info("list size: " + outputList.size());
		if (max != 0) {
			outList = outputList.stream().limit(max).collect(Collectors.toList());
		} else {
			outList = outputList;
		}
		logger.info("Limited list size: " + outList.size());

		int size = outList.size();
		if (size > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					publish(topic, map, outList);
				} else {
					outputAsFile(outList, map, topic, subExch);
				}
			} else {
				printList(outList);
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} else {
			logger.info("outList size is zero");
		}
//		System.out.println(storeService.updateAnalysisField("RFP", "20220502", "ind.obv", 21));
//		System.out.println("50 > 200 Counter:" + storeService.countByCriterion("this.ind.sma50>this.ind.sma200"));
//		System.out.println("21 > 50 Counter:" + storeService.countByCriterion("this.ind.sma21>this.ind.sma50"));

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_ANALYSIS_TRADEDATE, containerFactory = CONTAINER_FACTORY_LIST)
	public void getAnalysisDate(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_ANALYSIS_TRADEDATE, map.toString());
		final String format = map.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);

		List<Map<String, Object>> outputList = storeService.getDates();

		int size = outputList.size();
		if (size > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
//					publishList(outputList, map, ticker, topic, date, size, day);
				} else {
					outputAsFile(outputList, map, topic, "tradedates");
				}
			} else {
				logger.info("Size: " + size);
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} else {
			logger.info("outputList size is zero");
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_TRADEDATE_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void getTradeDateList(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_TRADEDATE_SINGLE, map.toString());

		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		List<Map<String, Object>> list = storeService.getTradeDateList(date);
		
		if (list.size()> 0) {
			getData(map, OBJECT_TYPE_TRADEDATE, date);
		}
	}
	
	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_ANALYSIS_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void getAnalysisList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_ANALYSIS_LIST, list.get(0).toString());

		getDataList(list, OBJECT_TYPE_ANALYSIS);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_HISTORICAL_LIST, list.get(0).toString());

		getDataList(list, OBJECT_TYPE_HISTORICAL);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_ANALYSIS_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void getAnalysis(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_ANALYSIS_SINGLE, map.toString());

		final String ticker = map.get(mapKey.getTicker()).toString();

		getData(map, OBJECT_TYPE_ANALYSIS, ticker);

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_HISTORICAL_SINGLE, containerFactory = CONTAINER_FACTORY_MAP)
	public void getHistorical(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_HISTORICAL_SINGLE, map.toString());

		final String ticker = map.get(mapKey.getTicker()).toString();

		getData(map, OBJECT_TYPE_HISTORICAL, ticker);

	}

	private void getData(Map<String, Object> map, int objType, String ticker) {

		final String format = map.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final Integer day = Integer.parseInt(map.getOrDefault(mapKey.getDay(), 0).toString());
		final String topic = getTopicFromList(map);

		List<Map<String, Object>> outputList = null;

		Calendar calendar = getCalendar(DATE_FORMAT, date);

		if (calendar != null) {
			calendar.add(Calendar.DATE, -day);
			final String formattedDate = String.format(OUTPUT_DATE_FORMAT, calendar);
			if (objType == OBJECT_TYPE_HISTORICAL) {
				outputList = storeService.getHistoricalList(ticker, formattedDate);
			} else if (objType == OBJECT_TYPE_ANALYSIS){
				outputList = storeService.getAnalysisList(ticker, formattedDate);
			} else {
				outputList = storeService.getAnalysisListByDate(date);
			}
		} else {
			if (objType == OBJECT_TYPE_HISTORICAL) {
				outputList = storeService.getHistoricalList(ticker);
			} else  if (objType == OBJECT_TYPE_ANALYSIS){
				outputList = storeService.getAnalysisList(ticker);
			} else {
				outputList = storeService.getAnalysisListByDate(date);
			}
		}

		int size = outputList.size();
		if (size > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					publishList(outputList, map, ticker, topic, date, size, day);
				} else {
					outputAsFile(outputList, map, topic, ticker);
				}
			} else {
				logger.info("Size: " + size);
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} else {
			logger.info("outputList size is zero");
		}
	}

	private void getDataList(List<Map<String, Object>> list, int objType) {
		final Map<String, Object> firstMap = list.get(0);

		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readFile(firstMap, currentTopic, subExch);
			list.add(0, firstMap);
		}

		list.stream().skip(1).forEach(x -> {
			Map<String, Object> newMap = firstMap.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			String ticker = x.get(mapKey.getTicker()).toString();
			newMap.put(mapKey.getTicker(), ticker);
			getData(newMap, objType, ticker);

		});
	}

	private void outputAsFile(List<Map<String, Object>> outputList, Map<String, Object> map, String topic,
			String fileName) {
		try {
			File file = getFile(topic, fileName);

			objectMapper.writeValue(file, outputList);

			publishAfterOutputAsFile(map, file, topic, outputList.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private File getFile(String topic, String fileName) {
		String[] topicBreakDown = topic.split(TOPIC_DELIMITER);
		String topicAction = DEFAULT_TOPIC_ACTION;
		String topicType = DEFAULT_TOPIC_TYPE;
		if (topicBreakDown.length >= 2) {
			topicAction = topicBreakDown[0];
			topicType = topicBreakDown[1];
		}

		return new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator + topicType
				+ File.separator + fileName + FILE_EXTENSION);

	}

	private void publishAfterOutputAsFile(Map<String, Object> map, File file, String topic, int size) {
		Map<String, Object> newMap = map.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		newMap.put(mapKey.getTotal(), size);
		if (file != null) {
			newMap.put(mapKey.getLength(), file.length());
		}
		List<Map<String, Object>> outList = new ArrayList<>();

		outList.add(0, newMap);
		publish(topic, outList);
	}

	private void publishList(List<Map<String, Object>> outputList, Map<String, Object> map, String ticker, String topic,
			String date, int size, int day) {
		final Integer limit = Integer.parseInt(map.getOrDefault(mapKey.getLimit(), 0).toString());

		if (topic != null) {
			if (size < limit) {

				logger.info("Send without processing: " + ticker);
				Map<String, Object> newMap = map.entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
				newMap.put(mapKey.getTicker(), ticker);
				newMap.put(mapKey.getTotal(), size);
				outputList.add(0, newMap);
				publish(topic, outputList);

			} else {

				logger.info("Processing before sending: " + ticker);
				int skip = 0;
				int count = 0;
				String oDate = date;
				while (size > (skip)) {
					List<Map<String, Object>> oList = outputList.stream().skip(skip).limit(limit)
							.collect(Collectors.toList());
					int oSize = oList.size();

					Map<String, Object> newMap = map.entrySet().stream()
							.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

					newMap.put(mapKey.getTicker(), ticker);
					newMap.put(mapKey.getTotal(), oSize);
					newMap.put(mapKey.getDate(), oDate);

					oList.add(0, newMap);

					if (oSize < limit) {
						skip += oSize;
					} else {
						Map<String, Object> oMap = oList.get(oList.size() - 1);
						oDate = oMap.get(mapKey.getDate()).toString();
						count++;
						skip = count * (limit - day);
					}

					publish(topic, oList);
				}

			} // if (size < limit)

		} else {

			logger.info("outputList size: " + outputList.size());
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());

		}
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
			logger.debug(e.toString());
		}
		return calendar;
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_SUMMARY_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummaryList(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_SUMMARY_LIST, list.get(0).toString());

		final Map<String, Object> firstMap = list.get(0);
		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String method = firstMap.getOrDefault(mapKey.getMethod(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);
		final String subExch = firstMap.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readFile(firstMap, currentTopic, subExch);
			list.add(0, firstMap);
		}
		Long start = System.currentTimeMillis();
		list.stream().parallel().skip(1).forEach(x -> {

			if (method.equals(DEFAULT_STRING_VALUE)) {
				updateSummary(x);
			} else {
				updateSummaryFromAllRecords(x);
			}

		});
		Long end = System.currentTimeMillis();
		logger.info("Total time for getting summary:" + (end - start));
		if (list.size() > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					publish(topic, list.remove(0), list);
				} else {
					outputAsFile(list, firstMap, topic, subExch);
				}
			} else {
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			logger.info("outputList size is zero");
		}
	}

	private void updateSummary(Map<String, Object> x) {
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
	}

	private void updateSummaryFromAllRecords(Map<String, Object> x) {

		String ticker = x.get(mapKey.getTicker()).toString();

		Map<String, Object> y = storeService.getHistoricalSummaryFromAllRecords(ticker);

		y.entrySet().forEach((i) -> {
			x.put(i.getKey(), i.getValue());
		});

		Double last = Double.valueOf(y.getOrDefault(mapKey.getLastP(), DEFAULT_DOUBLE_VALUE).toString());
		if (last != 0) {
			Long sharesO = Long.valueOf(x.getOrDefault(mapKey.getSharesO(), 0).toString());
			if (sharesO != 0) {
				x.put(mapKey.getMCap(), Double.valueOf(last * sharesO).longValue());
			}
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBGET_SUMMARY_SINGLE, containerFactory = CONTAINER_FACTORY_LIST)
	public void getHistoricalSummary(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBGET_SUMMARY_SINGLE, map.toString());

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
			logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_DBUPDATE_HISTORICAL_ALL, containerFactory = CONTAINER_FACTORY_MAP)
	public void updateHistoricalAll(Map<String, Object> map) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_DBUPDATE_HISTORICAL_ALL, map.toString());

		final String topic = getTopicFromList(map);

		Integer loop = Integer.valueOf(map.get("loop").toString());
		IntStream.range(0, loop).parallel().forEach(k -> {
			List<Map<String, Object>> outputList = storeService.getHistoricalList(k);

			System.out.println("Total:" + outputList.size());
			if (topic != null) {

				Map<String, Object> newMap = map.entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

				outputList.add(0, newMap);

				publish(topic, outputList);
			} else {
				logger.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}

		});

	}

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
}