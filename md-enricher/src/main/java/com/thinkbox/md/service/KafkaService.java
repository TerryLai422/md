package com.thinkbox.md.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaService {

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

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String TOPIC_CREATE_DAILYSUMMARY_LIST = "create.dailysummary.list";
	
	private final String TOPIC_ENRICH_EXCHANGE_LIST = "enrich.exchange.list";

	private final String TOPIC_ENRICH_HISTORICAL_LIST = "enrich.historical.list";

	private final String TOPIC_ENRICH_ANALYSIS_LIST = "enrich.analysis.list";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String FILE_EXTENSION = ".json";

	private final static String TOPIC_DELIMITER = "[.]";

	private final static String DEFAULT_TOPIC_ACTION = "unknown";

	private final static String DEFAULT_TOPIC_TYPE = "unknown";

	private final int BATCH_LIMIT = 1000;

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
//		log.info(STRING_LOGGER_SENT_MESSAGE, topic, list.toString());

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
	@KafkaListener(topics = TOPIC_CREATE_DAILYSUMMARY_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void createDailySummaryList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_CREATE_DAILYSUMMARY_LIST, list.get(0).toString());

		Map<String, Object> firstMap = list.get(0);

		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String date = firstMap.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readFile(firstMap, currentTopic, date);
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
	@KafkaListener(topics = TOPIC_ENRICH_EXCHANGE_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processExchangeData(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_ENRICH_EXCHANGE_LIST, list.get(0).toString());

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
	@KafkaListener(topics = TOPIC_ENRICH_ANALYSIS_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void enrichAnalysisList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_ENRICH_ANALYSIS_LIST, list.get(0).toString());

		enrichList(list, enrichService.OBJECT_TYPE_ANALYSIS);

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_ENRICH_HISTORICAL_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void enrichHistericalList(List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_ENRICH_HISTORICAL_LIST, list.get(0).toString());

//		List<Map<String, Object>> weeklyList = enrichService.consolidate(mapValue.getWeekly(), list);
//		weeklyList.forEach(System.out::println);
//		
//		List<Map<String, Object>> monthlyList = enrichService.consolidate(mapValue.getMonthly(), list);	
//		monthlyList.forEach(System.out::println);

		enrichList(list, enrichService.OBJECT_TYPE_HISTORICAL);
		
	}

	private void enrichList(List<Map<String, Object>> list, int type) {
		List<Map<String, Object>> outputList = null;

		Map<String, Object> firstMap = list.get(0);

		final String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		final String ticker = firstMap.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		final String currentTopic = getCurrentTopicFromList(firstMap);
		final String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			list = readFile(firstMap, currentTopic, ticker);
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
	
	private List<Map<String, Object>> readFile(Map<String, Object> firstMap, String currentTopic, String fileName) {

		List<Map<String, Object>> mapperList = null;
		try {
			String[] topicBreakDown = currentTopic.split(TOPIC_DELIMITER);
			String topicAction = DEFAULT_TOPIC_ACTION;
			String topicType = DEFAULT_TOPIC_TYPE;
			if (topicBreakDown.length >= 2) {
				topicAction = topicBreakDown[0];
				topicType = topicBreakDown[1];
			}
			File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator
					+ topicType + File.separator + fileName + FILE_EXTENSION);

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

	private void outputAsFile(List<Map<String, Object>> outputList, Map<String, Object> map, String topic, String fileName) {

		try {
			String[] topicBreakDown = topic.split(TOPIC_DELIMITER);
			String topicAction = DEFAULT_TOPIC_ACTION;
			String topicType = DEFAULT_TOPIC_TYPE;
			if (topicBreakDown.length >= 2) {
				topicAction = topicBreakDown[0];
				topicType = topicBreakDown[1];
			}
			File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator
					+ topicType + File.separator + fileName + FILE_EXTENSION);
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
			File file = new File(System.getProperty(USER_HOME) + File.separator + topicAction + File.separator
					+ topicType + File.separator + fileName + FILE_EXTENSION);
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