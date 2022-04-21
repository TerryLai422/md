package com.thinkbox.md.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.md.config.JsonProperties;
import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;
import com.thinkbox.md.util.CSVFileReader;

@Component
public class FileParseService {

	private final Logger logger = LoggerFactory.getLogger(FileParseService.class);

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final static String DETAIL_DIRECTORY = "detail";

	private final static String HISTORICAL_DIRECTORY = "historical";

	private final static String EXCHANGE_DIRECTORY = "exchange";

	private final static String INFO_DIRECTORY = "info";

	private final static String DETAIL_FILE_SUFFIX = "-detail";

	private final static String INFO_FILE_SUFFIX = "-info";

	private final static String HISTORICAL_DAILY_FILE_SUFFIX = "-d-historical";

	private final static String FILE_EXTENSION = ".txt";

	private final static Character COMMA_SEPERATOR = ',';

	private final static Character TAB_SEPERATOR = '\t';

	private final static String OUTPUT_DATE_FORMAT = "%1$tY%1$tm%1$td";

	private final static String DEFAULT_TIME_VALUE = "000000";
	
	@Value("${app.data.directory:-}")
	private String dataDirectory;

	@Autowired
	private MapKeyParameter mapKey;

	@Autowired
	private MapValueParameter mapValue;

	@Autowired
	private JsonProperties jsonProperties;

	@PostConstruct
	public void init() {
		if (dataDirectory != null && dataDirectory.equals("-")) {
			dataDirectory = System.getProperty(USER_HOME);
		}
	}

	public List<String> getSymbols(final String subExchange) throws IOException {

		String directory = dataDirectory + File.separator + DETAIL_DIRECTORY + File.separator + subExchange;

		logger.info(directory);
		File directoryPath = new File(directory);

		return Stream.of(directoryPath.listFiles()).filter(x -> !x.isDirectory()).map(x -> {
			String name = x.getName();
			return name.substring(0, name.length() - 11);
		}).collect(Collectors.toList());

	}

	public Map<String, Object> parseDetailFile(final String subExchange, final String symbol) throws IOException {

		final String suffix = (subExchange.equals("TSX")) ? ".TO" : (subExchange.equals("TSXV")) ? ".V" : "";

		String fileName = dataDirectory + File.separator + DETAIL_DIRECTORY + File.separator + subExchange
				+ File.separator + symbol + DETAIL_FILE_SUFFIX + FILE_EXTENSION;

		logger.info(fileName);

		File file = new File(fileName);

		InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
		JsonNode node = objectMapper.readTree(inputStreamReader);

		Map<String, Object> map = new TreeMap<>();
		jsonProperties.getProperty().forEach((x, y) -> {
			Object object = getNodeValue(node, y);
			if (object != null) {
				map.put(x, object);
			}
		});
		map.put(mapKey.getSymbol(), symbol.replaceAll(suffix, ""));
		map.put(mapKey.getSubExchange(), subExchange);

		return map;
	}

	private Object getNodeValue(JsonNode node, List<String> list) {
		boolean has = true;
		for (String name : list) {
			if (node.has(name)) {
				node = node.get(name);
			} else {
				has = false;
				break;
			}
		}

		if (has) {
			if (node.isNumber()) {
				if (node.isDouble()) {
					return node.asDouble();
				} else if (node.isInt()) {
					return node.asInt();
				}
			} else if (node.isBoolean()) {
				return node.asBoolean();
			}
			return node.asText();
		}
		return null;
	}

	private String getDateFormat(final String dataSource) {
		String format;
		if (dataSource.equals("yahoo")) {
			format = "yyyy-MM-dd";
		} else {
			format = "yyyyMMdd";
		}

		return format;
	}

	private List<Integer> getColumnsPosition(final String dataSource) {
		List<Integer> columns = null;
		if (dataSource.equals("yahoo")) {
			columns = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
		} else {
			columns = Arrays.asList(2, 4, 5, 6, 7, 7, 8);
		}

		return columns;
	}

	private String getFileName(final String dataType, final String symbol, final String ticker) {
		String fileName = null;
		if (dataType.equals("yahooHistorical")) {
			fileName = dataDirectory + File.separator + HISTORICAL_DIRECTORY + File.separator + ticker
					+ HISTORICAL_DAILY_FILE_SUFFIX + FILE_EXTENSION;
		} else {
			fileName = dataDirectory + File.separator + HISTORICAL_DIRECTORY + File.separator + symbol + ".us"
					+ FILE_EXTENSION;
		}
		return fileName;
	}

	private int getTypePosition(final String dataSource) {

		if (dataSource.equals("stooq")) {
			return 1;
		}
		return -1;
	}

	private int getTimePosition(final String dataSource) {

		if (dataSource.equals("stooq")) {
			return 3;
		}
		return -1;
	}

	public List<Map<String, Object>> parseHistoricalFile(final String dataSource, final String symbol,
			final String ticker) throws IOException {

		final List<Integer> columns = getColumnsPosition(dataSource);
		final String dateFormat = getDateFormat(dataSource);
		final int typePosition = getTypePosition(dataSource);
		final int timePosition = getTimePosition(dataSource);
		String fileName = getFileName(dataSource, symbol, ticker);
		logger.info(fileName);

		CSVFileReader csvFileReader = new CSVFileReader();

		List<String[]> list = csvFileReader.read(fileName, COMMA_SEPERATOR, null);

		List<Map<String, Object>> outputList = list.stream().map(x -> {
			Map<String, Object> map = null;

			try {
				Calendar calendar = null;
				String date = x[columns.get(0)];
				Date sDate;
				sDate = new SimpleDateFormat(dateFormat).parse(date);
				calendar = Calendar.getInstance();
				calendar.setTime(sDate);
				calendar.set(Calendar.MILLISECOND, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.HOUR, 0);

				int year = calendar.get(Calendar.YEAR);
				int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
				int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);

				map = new TreeMap<String, Object>();

				if (typePosition == -1) {
					map.put(mapKey.getType(), new String(mapValue.getDaily()));
				} else {
					map.put(mapKey.getType(), new String(x[typePosition]));
				}
				map.put(mapKey.getTicker(), new String(ticker).toUpperCase());
				map.put(mapKey.getSymbol(), new String(symbol).toUpperCase());
				map.put(mapKey.getDate(), new String(String.format(OUTPUT_DATE_FORMAT, calendar)));
				if (timePosition == -1) {
					map.put(mapKey.getTime(), DEFAULT_TIME_VALUE);
				} else {
					map.put(mapKey.getTime(), new String(x[timePosition]));
				}				
				map.put(mapKey.getYear(), year);
				map.put(mapKey.getMonth(), calendar.get(Calendar.MONTH) + 1);
				map.put(mapKey.getDay(), calendar.get(Calendar.DATE));
				map.put(mapKey.getDayOfYear(), dayOfYear);
				map.put(mapKey.getWeekOfYear(), weekOfYear);
				map.put(mapKey.getDayOfWeek(), calendar.get(Calendar.DAY_OF_WEEK));
				if (weekOfYear == 1 && ((year % 4 != 0 && dayOfYear >= 362)
						|| (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) && dayOfYear >= 363))) {
					map.put(mapKey.getYearForWeek(), year + 1);
				} else {
					map.put(mapKey.getYearForWeek(), year);
				}
				map.put(mapKey.getOpen(), Double.parseDouble(x[columns.get(1)]));
				map.put(mapKey.getHigh(), Double.parseDouble(x[columns.get(2)]));
				map.put(mapKey.getLow(), Double.parseDouble(x[columns.get(3)]));
				map.put(mapKey.getClose(), Double.parseDouble(x[columns.get(4)]));
				map.put(mapKey.getAdjClose(), Double.parseDouble(x[columns.get(5)]));
				map.put(mapKey.getVolume(), Long.parseLong(x[columns.get(6)]));

			} catch (ParseException e) {
				logger.info(e.toString());
			}
			return map;
		}).filter(x -> x != null).collect(Collectors.toList());

		Map<String, Object> first = outputList.get(0);
		Map<String, Object> last = outputList.get(outputList.size() - 1);

		Map<String, Object> index = new TreeMap<String, Object>();

		index.put(mapKey.getType(), new String(mapValue.getDaily()));
		index.put(mapKey.getTicker(), new String(ticker).toUpperCase());
		index.put(mapKey.getSymbol(), new String(symbol).toUpperCase());
		index.put(mapKey.getFromDate(), first.get(mapKey.getDate()));
		index.put(mapKey.getFromYear(), first.get(mapKey.getYear()));
		index.put(mapKey.getFromMonth(), first.get(mapKey.getMonth()));
		index.put(mapKey.getFromDay(), first.get(mapKey.getDay()));
		index.put(mapKey.getFromWeekOfYear(), first.get(mapKey.getWeekOfYear()));
		index.put(mapKey.getFromDayOfWeek(), first.get(mapKey.getDayOfWeek()));

		index.put(mapKey.getToDate(), last.get(mapKey.getDate()));
		index.put(mapKey.getToYear(), last.get(mapKey.getYear()));
		index.put(mapKey.getToMonth(), last.get(mapKey.getMonth()));
		index.put(mapKey.getToDay(), last.get(mapKey.getDay()));
		index.put(mapKey.getToWeekOfYear(), last.get(mapKey.getWeekOfYear()));
		index.put(mapKey.getToDayOfWeek(), last.get(mapKey.getDayOfWeek()));

		index.put(mapKey.getTotal(), Long.valueOf(outputList.size()));

		outputList.add(0, index);

		return outputList;
	}

	public List<Map<String, Object>> parseExchangeFile(final String exchange) throws IOException {

		String fileName = dataDirectory + File.separator + EXCHANGE_DIRECTORY + File.separator + exchange
				+ FILE_EXTENSION;

		logger.info(fileName);

		final String suffix = (exchange.equals("TSX")) ? ".TO" : (exchange.equals("TSXV")) ? ".V" : "";

		final boolean neededSuffix = (exchange.equals("TSX") || exchange.equals("TSXV")) ? true : false;

		CSVFileReader csvFileReader = new CSVFileReader();

		List<String[]> list = csvFileReader.read(fileName, TAB_SEPERATOR, null);

		List<Map<String, Object>> mapList = list.stream().map(x -> {

			String symbol = x[0];
			String ticker = symbol;

			Map<String, Object> map = new TreeMap<String, Object>();
			map.put(mapKey.getSymbol(), x[0]);
			map.put(mapKey.getName(), x[1]);
			map.put(mapKey.getExchange(), new String(exchange));

			if (neededSuffix) {
				long count = symbol.chars().filter(ch -> ch == '.').count();
				if (count == 0) {
					ticker = symbol + suffix;
				} else if (count == 1) {
					ticker = symbol.replace('.', '-') + suffix;
				} else {
					ticker = "-";
				}
			}

			map.put(mapKey.getTicker(), ticker);

			return map;
		}).collect(Collectors.toList());

		Map<String, Object> index = new TreeMap<String, Object>();
		index.put(mapKey.getExchange(), new String(exchange));
		index.put(mapKey.getTotal(), Long.valueOf(mapList.size()));

		mapList.add(0, index);

		return mapList;
	}

	public Map<String, Object> parseInfoFile(String symbol) throws IOException {

		String fileName = dataDirectory + File.separator + INFO_DIRECTORY + File.separator + symbol + INFO_FILE_SUFFIX
				+ FILE_EXTENSION;

		File file = new File(fileName);

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map = mapper.readValue(file, new TypeReference<Map<String, Object>>() {
		});

		return map;
	}
}