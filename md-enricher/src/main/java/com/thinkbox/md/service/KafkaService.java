package com.thinkbox.md.service;

import static com.thinkbox.md.service.EnrichService.OBJECT_TYPE_ANALYSIS;
import static com.thinkbox.md.service.EnrichService.OBJECT_TYPE_HISTORICAL;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;
//import com.thinkbox.md.config.MapValueParameter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class KafkaService {

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

//	@Autowired
//	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;

	@Autowired
	private EnrichService enrichService;

	@Autowired
	private MapKeyParameter mapKey;

	@Value("${kafka.topic.create-dailysummary-list}")
	private String topicCreateDailySummaryList;

	@Value("${kafka.topic.enrich-exchange-list}")
	private String topicEnrichExchangeList;

	@Value("${kafka.topic.enrich-analysis-list}")
	private String topicEnrichAnalysisList;

	@Value("${kafka.topic.enrich-historical-list}")
	private String topicEnrichHistoricalList;

//	@Autowired
//	private MapValueParameter mapValue;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final String ASYNC_EXECUTOR = "asyncExecutor";

//	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String FILE_EXTENSION = ".json";

	private final static String TOPIC_DELIMITER = "[-]";

	private final static String STRING_EMPTY_SPACE = "";

	private final static String OUTPUT_FORMAT_JSON = "JSON";

	private final static String STRING_COMMA = ",";

	private final static String STRING_SQUARE_OPEN_BRACKET = "[";

	private final static String STRING_SQUARE_CLOSE_BRACKET = "]";

	private final static String STRING_CURLY_BRACKET = "{}";

	private final static String DEFAULT_TOPIC_ACTION = "unknown";

	private final static String DEFAULT_TOPIC_TYPE = "unknown";

	private final int BATCH_LIMIT = 1000;

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, list.toString());

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
	@KafkaListener(topics = "${kafka.topic.create-dailysummary-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void createDailySummaryList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicCreateDailySummaryList, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);

//		final String dataFormat = firstMap.getOrDefault(mapKey.getDataFormat(), DEFAULT_STRING_VALUE).toString();
		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String date = firstMap.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readJSONFile(firstMap, currentTopic, date);
		}

		Map<String, Object> outMap = enrichService.createDailySummary(list);
		if (outMap != null) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					List<Map<String, Object>> outputList = new ArrayList<>();
					outputList.add(outMap);
					publish(topic, firstMap, outputList);
				} else {
					outputAsFile(outMap, firstMap, topic, date);
				}
			} else {
				log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			log.info("outMap is null");
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.enrich-exchange-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void processExchangeData(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicEnrichExchangeList, list.get(0).toString());

		List<Map<String, Object>> outputList = enrichService.enrichExchange(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);

		if (topic != null) {
			final Map<String, Object> first = outputList.remove(0);
			firstMap.forEach((x, y) -> {
				first.put(x, y);
			});

			outputList.add(0, first);
//			outputList.forEach(System.out::println);

			publish(topic, outputList);
		} else {
//			outputList.forEach(System.out::println);
			log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.enrich-analysis-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void enrichAnalysisList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicEnrichAnalysisList, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);

		final String method = firstMap.getOrDefault(mapKey.getMethod(), DEFAULT_STRING_VALUE).toString();

		if (method.equals(DEFAULT_STRING_VALUE)) {
			enrichList(OBJECT_TYPE_ANALYSIS, list);
		} else {
			enrichFlux(OBJECT_TYPE_ANALYSIS, firstMap);
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.enrich-historical-list}", containerFactory = CONTAINER_FACTORY_LIST)
	public void enrichHistericalList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicEnrichHistoricalList, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);

		final String method = firstMap.getOrDefault(mapKey.getMethod(), DEFAULT_STRING_VALUE).toString();

//		List<Map<String, Object>> weeklyList = enrichService.consolidate(mapValue.getWeekly(), list);
//		weeklyList.forEach(System.out::println);
//		
//		List<Map<String, Object>> monthlyList = enrichService.consolidate(mapValue.getMonthly(), list);	
//		monthlyList.forEach(System.out::println);

		log.info("Method:" + method);
		if (method.equals(DEFAULT_STRING_VALUE)) {
			enrichList(OBJECT_TYPE_HISTORICAL, list);
		} else {
			enrichFlux(OBJECT_TYPE_HISTORICAL, firstMap);
		}

	}

	private void enrichFlux(int type, Map<String, Object> map) {

		final String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(map);
		final String topic = getTopicFromList(map);

		final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		Flux<Map<String, Object>> flux = enrichService.enrichFlux(type, date, getFullFileName(requestID, currentTopic, ticker));

		Mono<Long> count = flux.count();
		long size = count.block();
		if (size > 0) {
			if (topic != null) {
				processFlux(flux, map, topic, ticker, size);
			} else {
				log.info("Flux Size: " + size);
				log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} else {
			log.info("outputList size is zero");
		}
	}

	private void processFlux(Flux<Map<String, Object>> flux, final Map<String, Object> map, final String topic,
			final String fileName, final Long size) {

		final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		final String outFileName = getFullFileName(requestID, topic, fileName);
		final String dataFormat = map.getOrDefault(mapKey.getDataFormat(), DEFAULT_STRING_VALUE).toString();

		Path opPath = Paths.get(outFileName);
		BufferedWriter bw;
		try {
			bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE);
			if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
				bw.write(STRING_SQUARE_OPEN_BRACKET);
			}
			flux.subscribe(s -> write(dataFormat, bw, s), (e) -> close(bw), // close file if error / oncomplete
					() -> complete(dataFormat, bw, map, topic, size));

		} catch (IOException e) {
			log.info("Exception when processing Flux: " + e.toString());
			e.printStackTrace();
		}
	}

	private void close(Closeable closeable) {
		try {
//			log.info("close");
			closeable.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void complete(String dataFormat, BufferedWriter bw, Map<String, Object> map, String topic, long size) {
		try {
//			log.info("complete");
			if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
				bw.write(STRING_CURLY_BRACKET + STRING_SQUARE_CLOSE_BRACKET);
			}
			bw.close();
			if (topic != null) {
				publishAfterOutputAsFile(map, topic, size);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void write(String dataFormat, BufferedWriter bw, Map<String, Object> map) {
		try {
			bw.write(objectMapper.writeValueAsString(map)
					+ (dataFormat.equals(OUTPUT_FORMAT_JSON) ? STRING_COMMA : STRING_EMPTY_SPACE));
			bw.newLine();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void publishAfterOutputAsFile(Map<String, Object> map, String topic, long size) {
		publishAfterOutputAsFile(map, null, topic, size);
	}

	private void publishAfterOutputAsFile(Map<String, Object> map, File file, String topic, long size) {
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

	private void enrichList(int type, List<Map<String, Object>> list) {
		List<Map<String, Object>> outputList = null;

		Map<String, Object> firstMap = list.get(0);

		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String ticker = firstMap.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readJSONFile(firstMap, currentTopic, ticker);
		}
		outputList = enrichService.enrichList(list, type);

		int size = outputList.size();
		if (size > 0) {
			if (topic != null) {
				if (format.equals(DEFAULT_STRING_VALUE)) {
					publish(topic, firstMap, outputList);
				} else {
					outputAsFile(outputList, firstMap, topic, ticker);
				}
			} else {
				log.info(STRING_LOGGER_FINISHED_MESSAGE, firstMap.toString());
			}
		} else {
			log.info("outputList size is zero");
		}
	}

	private String getFullFileName(String requestID, String currentTopic, String fileName) {
		String[] topicBreakDown = currentTopic.split(TOPIC_DELIMITER);
		String topicAction = DEFAULT_TOPIC_ACTION;
		String topicType = DEFAULT_TOPIC_TYPE;
		if (topicBreakDown.length >= 2) {
			topicAction = topicBreakDown[0];
			topicType = topicBreakDown[1];
		}
		return System.getProperty(USER_HOME) + File.separator + topicAction + File.separator + topicType
				+ File.separator + requestID + "." + fileName + FILE_EXTENSION;

	}

	private List<Map<String, Object>> readJSONFile(Map<String, Object> firstMap, String currentTopic, String fileName) {

		List<Map<String, Object>> mapperList = null;
		try {
			final String requestID = firstMap.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
			File file = new File(getFullFileName(requestID, currentTopic, fileName));

			mapperList = objectMapper.readValue(new FileInputStream(file),
					new TypeReference<List<Map<String, Object>>>() {
					});
			mapperList.add(0, firstMap);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapperList;
	}

	private void outputAsFile(List<Map<String, Object>> outputList, Map<String, Object> map, String topic,
			String fileName) {
		log.info("outputAsFile");
		try {
			String[] topicBreakDown = topic.split(TOPIC_DELIMITER);
			String topicAction = DEFAULT_TOPIC_ACTION;
			String topicType = DEFAULT_TOPIC_TYPE;
			if (topicBreakDown.length >= 2) {
				topicAction = topicBreakDown[0];
				topicType = topicBreakDown[1];
			}
			final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
			File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator
					+ topicType + File.separator + requestID + "." + fileName + FILE_EXTENSION);
			objectMapper.writeValue(file, outputList);

			Map<String, Object> newMap = map.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//			newMap.put(mapKey.getTicker(), ticker);
			newMap.put(mapKey.getTotal(), outputList.size());

			if (file != null) {
				newMap.put(mapKey.getLength(), file.length());
			}
			List<Map<String, Object>> outList = new ArrayList<>();

			outList.add(0, newMap);
			publish(topic, outList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void outputAsFile(Map<String, Object> outMap, Map<String, Object> map, String topic, String fileName) {

		try {
			String[] topicBreakDown = topic.split(TOPIC_DELIMITER);
			String topicAction = DEFAULT_TOPIC_ACTION;
			String topicType = DEFAULT_TOPIC_TYPE;
			if (topicBreakDown.length >= 2) {
				topicAction = topicBreakDown[0];
				topicType = topicBreakDown[1];
			}
			final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
			File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator
					+ topicType + File.separator + requestID + "." + fileName + FILE_EXTENSION);
			objectMapper.writeValue(file, outMap);

			Map<String, Object> newMap = map.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//			newMap.put(mapKey.getTicker(), ticker);
			newMap.put(mapKey.getTotal(), 1);

			if (file != null) {
				newMap.put(mapKey.getLength(), file.length());
			}
			List<Map<String, Object>> outList = new ArrayList<>();

			outList.add(0, newMap);
			publish(topic, outList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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