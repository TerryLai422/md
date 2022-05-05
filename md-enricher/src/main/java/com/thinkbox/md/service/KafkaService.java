package com.thinkbox.md.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

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

	private final String TOPIC_ENRICH_EXCHANGE_DATA_LIST = "enrich.exchange.list";

	private final String TOPIC_ENRICH_HISTORICAL_DATA_LIST = "enrich.historical.list";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final String CONTAINER_FACTORY_LIST = "listListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final int BATCH_LIMIT = 1000;

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
//		logger.info(STRING_LOGGER_SENT_MESSAGE, topic, list.toString());

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
	@KafkaListener(topics = TOPIC_ENRICH_EXCHANGE_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processExchangeData(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_ENRICH_EXCHANGE_DATA_LIST, list.get(0).toString());

		List<Map<String, Object>> outputList = enrichService.enrichExchange(list);

		Map<String, Object> firstMap = list.get(0);
		String topic = getTopicFromList(firstMap);

		if (topic != null) {
			final Map<String, Object> first = outputList.remove(0);
			firstMap.forEach((x, y) -> {
				first.put(x, y);
			});

			outputList.add(0, first);
			outputList.forEach(System.out::println);

			publish(topic, outputList);
		} else {
			outputList.forEach(System.out::println);
			logger.info("Finish Last Step: {}", firstMap.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = TOPIC_ENRICH_HISTORICAL_DATA_LIST, containerFactory = CONTAINER_FACTORY_LIST)
	public void processHistericalData(List<Map<String, Object>> list) {
		logger.info(STRING_LOGGER_RECEIVED_MESSAGE, TOPIC_ENRICH_HISTORICAL_DATA_LIST, list.get(0).toString());

//		List<Map<String, Object>> weeklyList = enrichService.consolidate(mapValue.getWeekly(), list);
//		weeklyList.forEach(System.out::println);
//		
//		List<Map<String, Object>> monthlyList = enrichService.consolidate(mapValue.getMonthly(), list);	
//		monthlyList.forEach(System.out::println);
		Map<String, Object> firstMap = list.get(0);

		String format = firstMap.getOrDefault(mapKey.getFormat(), DEFAULT_STRING_VALUE).toString();
		List<Map<String, Object>> outputList = null;
		String topic = getTopicFromList(firstMap);

		if (!format.equals(DEFAULT_STRING_VALUE)) {
			System.out.println("MMAP: " + firstMap.toString());
			processFile(firstMap, topic);
		} else {
			outputList = enrichService.enrichHistorical(list);

//		outputList.forEach(System.out::println);

			if (topic != null) {
//			outputList.add(0, firstMap);
				System.out.println("Size:" + outputList.size());
//			outputList.forEach(System.out::println);
				publish(topic, firstMap, outputList);
			} else {
				System.out.println("Size:" + outputList.size());
//			outputList.forEach(x -> {
//				logger.info(x.toString());
//			});
				logger.info("Finish Last Step: {}", firstMap.toString());
			}

		}
	}

	private void processFile(Map<String, Object> firstMap, String topic) {
		String ticker = firstMap.getOrDefault(mapKey.getTicker(), "-").toString();

		File file = new File(System.getProperty(USER_HOME) + File.separator +  "enrich" +  File.separator + ticker + ".json");
		try {
			List<Map<String, Object>> mapperList = objectMapper.readValue(new FileInputStream(file),
					new TypeReference<List<Map<String, Object>>>() {
					});
			System.out.println("MapperList Size: " + mapperList.size());
			mapperList.add(0, firstMap);
			List<Map<String, Object>> outputList = enrichService.enrichHistorical(mapperList);
			File outfile = new File(System.getProperty(USER_HOME) + File.separator + "save" +  File.separator + ticker + ".json");
			objectMapper.writeValue(outfile, outputList);

			if (topic != null) {

				Map<String, Object> newMap = firstMap.entrySet().stream()
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
				newMap.put(mapKey.getTicker(), ticker);
				newMap.put(mapKey.getTotal(), outputList.size());

				if (outfile != null) {
					newMap.put(mapKey.getLength(), outfile.length());
				}
				List<Map<String, Object>> outList = new ArrayList<>();

				outList.add(0, newMap);
				publish(topic, outList);

			} else {
				if (outfile != null) {
					logger.info("File Size: " + outfile.length());
				}
				logger.info("outputList size: " + outputList.size());
				logger.info("Finish Last Step: {}", firstMap.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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