package com.thinkbox.md.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.MapKeyParameter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Service
@Slf4j
public class KafkaService {

	@Autowired
	private KafkaTemplate<String, List<Map<String, Object>>> kafkaTemplateList;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplateMap;

	@Autowired
	private MapKeyParameter mapKey;

	@Autowired
	private FileParseService fileParseService;

	@Value("${kafka.topic.parse-daily-list}")
	private String topicParseDailyList;

	@Value("${kafka.topic.parse-daily-single}")
	private String topicParseDailySingle;

	@Value("${kafka.topic.parse-historical-list}")
	private String topicParseHistoricalList;

	@Value("${kafka.topic.parse-detail-list}")
	private String topicParseDetailList;

	@Value("${kafka.topic.parse-detail-single}")
	private String topicParseDetailSingle;

	@Value("${kafka.topic.parse-info-single}")
	private String topicParseInfoSingle;

	@Value("${kafka.topic.parse-exchange-data}")
	private String topicParseExchangeData;

	@Value("${kafka.topic.parse-historical-single}")
	private String topicParseHistoricalSingle;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final String ASYNC_EXECUTOR = "asyncExecutor";

	private final String CONTAINER_FACTORY_MAP = "mapListener";

	private final static String STRING_LOGGER_SENT_MESSAGE = "Sent topic: {} -> {}";

	private final static String STRING_LOGGER_RECEIVED_MESSAGE = "Received topic: {} -> parameter: {}";

	private final static String STRING_LOGGER_FINISHED_MESSAGE = "Finish Last Step: {}";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String TOPIC_DELIMITER = "[-]";

	private final static String DEFAULT_TOPIC_ACTION = "unknown";

	private final static String DEFAULT_TOPIC_TYPE = "unknown";

	private final static String OUTPUT_FORMAT_JSON = "JSON";

	private final static String USER_HOME = "user.home";

	private final static String STRING_PERIOD = ".";
	
	private final static String STRING_COMMA = ",";

	private final static String STRING_SQUARE_OPEN_BRACKET = "[";

	private final static String STRING_SQUARE_CLOSE_BRACKET = "]";

	private final static String STRING_CURLY_BRACKET = "{}";

	private final static String FILE_EXTENSION_JSON = ".json";

	private final int BATCH_LIMIT = 2000;

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, List<Map<String, Object>> list) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, list.toString());

		kafkaTemplateList.send(topic, list);
	}

	@Async(ASYNC_EXECUTOR)
	public void publish(String topic, Map<String, Object> map) {
		log.info(STRING_LOGGER_SENT_MESSAGE, topic, map.toString());

		kafkaTemplateMap.send(topic, map);
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-exchange-data}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseExchange(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseExchangeData, map.toString());

		try {
			String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

			List<Map<String, Object>> outputList = fileParseService.parseExchangeFile(subExch);

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
				log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} catch (IOException e) {
			log.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-detail-list}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetailList(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseDetailList, map.toString());

		try {
			final String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

			List<String> list = fileParseService.getSymbolsfromDetailDirectory(subExch);

			final String topic = getTopicFromList(map);

			list.stream().forEach(x -> {
				Map<String, Object> outputMap;
				try {
					outputMap = fileParseService.parseDetailFile(subExch, x);

					if (topic != null) {
						List<Map<String, Object>> outputList = new ArrayList<>();

						outputList.add(0, map);
						outputList.add(outputMap);

						outputList.forEach(System.out::println);

						publish(topic, outputList);
					} else {
						log.info(outputMap.toString());
						log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
					}
				} catch (IOException e) {
					log.info(e.toString());
				}
			});
		} catch (

		IOException e) {
			log.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-detail-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDetail(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseDetailSingle, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
			String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();

			Map<String, Object> outputMap = fileParseService.parseDetailFile(subExch, ticker);

			String topic = getTopicFromList(map);

			if (topic != null) {
				List<Map<String, Object>> outputList = new ArrayList<>();

				outputList.add(0, map);
				outputList.add(1, outputMap);
				publish(topic, outputList);
			} else {
				log.info(outputMap.toString());
				log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}
		} catch (IOException e) {
			log.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-historical-list}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHistoricalList(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseHistoricalList, map.toString());

		try {
			final String directory = map.getOrDefault(mapKey.getDirectory(), DEFAULT_STRING_VALUE).toString();

			List<Map<String, Object>> list = fileParseService.getSymbolsfromHistoricalDirectory(directory);
			list.forEach(System.out::println);

			list.stream().parallel().forEach(x -> {

				map.forEach((i, j) -> {
					x.put(i, j);
				});

				parseHistericalData(x);
			});

		} catch (IOException e) {
			log.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-historical-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseHisterical(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseHistoricalSingle, map.toString());

		parseHistericalData(map);
	}

	private void parseHistericalData(Map<String, Object> map) {
		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
			String symbol = map.getOrDefault(mapKey.getSymbol(), DEFAULT_STRING_VALUE).toString();
			String dataSource = map.getOrDefault(mapKey.getDataSource(), DEFAULT_STRING_VALUE).toString();
			String directory = map.getOrDefault(mapKey.getDirectory(), DEFAULT_STRING_VALUE).toString();

			final String topic = getTopicFromList(map);

			// <List>String next = (List<String>) map.get("next");
			List<Map<String, Object>> outputList = fileParseService.parseHistoricalFile(directory, dataSource, symbol,
					ticker);

			if (topic != null) {
				log.info("Number of records:" + outputList.size());
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
				log.info(outputList.toString());
				log.info(STRING_LOGGER_FINISHED_MESSAGE, map.toString());
			}

		} catch (IOException e) {
			log.info(e.toString());
		}
	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-daily-list}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDailyList(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseDailyList, map.toString());

		try {
			final String directory = map.getOrDefault(mapKey.getDirectory(), DEFAULT_STRING_VALUE).toString();

			List<String> files = fileParseService.getSymbolsfromDailyDirectory(directory);
			files.forEach(log::info);

			final int numberOfFiles = files.size();

			files.stream().parallel().forEach(x -> {
				final Map<String, Object> fileMap = new TreeMap<>();
				map.forEach((i, j) -> {
					fileMap.put(i, j);
				});
				fileMap.put(mapKey.getFiles(), numberOfFiles);
				fileMap.put(mapKey.getFileName(), x);
				parseDailyDataAndSaveAsFile(fileMap);
			});

		} catch (IOException e) {
			log.info(e.toString());
		}

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-daily-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseDaily(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseDailySingle, map.toString());

		parseDailyDataAndSaveAsFile(map);

	}

	@Async(ASYNC_EXECUTOR)
	@KafkaListener(topics = "${kafka.topic.parse-info-single}", containerFactory = CONTAINER_FACTORY_MAP)
	public void parseInfo(Map<String, Object> map) {
		log.info(STRING_LOGGER_RECEIVED_MESSAGE, topicParseInfoSingle, map.toString());

		try {
			String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
			Map<String, Object> outMap = fileParseService.parseInfoFile(ticker);
//			list.forEach(System.out::println);
//			publish(TOPIC_PROCESS_INFO_DATA, outMap);
		} catch (IOException e) {
			log.info(e.toString());
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

	private String getOutFullFileName(String requestID, String topic, String fileName) {
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

		newMap.put(mapKey.getTotal(), size);
		if (file != null) {
			newMap.put(mapKey.getLength(), file.length());
		}
		List<Map<String, Object>> outList = new ArrayList<>();

		outList.add(0, newMap);
		publish(topic, outList);
	}

	private void parseDailyDataAndSaveAsFile(Map<String, Object> map) {

		final String symbol = DEFAULT_STRING_VALUE;
		final String ticker = DEFAULT_STRING_VALUE;

		final String subDirectory = map.getOrDefault(mapKey.getDirectory(), DEFAULT_STRING_VALUE).toString();
		final String dataSource = map.getOrDefault(mapKey.getDataSource(), DEFAULT_STRING_VALUE).toString();
		final String fileName = map.getOrDefault(mapKey.getFileName(), DEFAULT_STRING_VALUE).toString();
		final String dataFormat = map.getOrDefault(mapKey.getDataFormat(), DEFAULT_STRING_VALUE).toString();
		final String requestID = map.getOrDefault(mapKey.getRequestID(), DEFAULT_STRING_VALUE).toString();
		final String topic = getTopicFromList(map);
		final String inFilePath = fileParseService.getDailyFullFileName(subDirectory, fileName, dataSource, symbol,
				ticker);
		final String outFilePath = getOutFullFileName(requestID, topic, fileName);
		final List<Integer> columns = fileParseService.getColumnsPosition(dataSource);
		final String dateFormat = fileParseService.getDateFormat(dataSource);
		final int intervalPosition = fileParseService.getIntervalPosition(dataSource);
		final int timePosition = fileParseService.getTimePosition(dataSource);

		final Path inPath = Paths.get(inFilePath);
		final Path outPath = Paths.get(outFilePath);

		try {
			BufferedWriter bw;
			bw = Files.newBufferedWriter(outPath, StandardOpenOption.CREATE);
			bw.write(STRING_SQUARE_OPEN_BRACKET);
			final Flux<Map<String, Object>> flux = Flux
					.using(() -> Files.lines(inPath), Flux::fromStream, BaseStream::close).skip(1)
					.map(s -> fileParseService.parseStringArray(s.split(STRING_COMMA), columns, dateFormat,
							intervalPosition, timePosition, symbol, ticker));
			flux.subscribe(s -> write(bw, s), (e) -> close(bw), () -> complete(flux, dataFormat, bw, map, topic, 0));

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	private void close(BufferedWriter bw) {
		try {
			bw.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void complete(Flux<Map<String, Object>> flux, String dataFormat, BufferedWriter bw, Map<String, Object> map,
			String topic, long size) {
		try {
			if (dataFormat.equals(OUTPUT_FORMAT_JSON)) {
				bw.write(STRING_CURLY_BRACKET + STRING_SQUARE_CLOSE_BRACKET);
			}
			bw.close();
			if (topic != null) {
				publishAfterOutputAsFile(map, topic, flux.count().block());
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void write(BufferedWriter bw, Map<String, Object> map) {
		try {
			bw.write(objectMapper.writeValueAsString(map) + STRING_COMMA);
			bw.newLine();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}