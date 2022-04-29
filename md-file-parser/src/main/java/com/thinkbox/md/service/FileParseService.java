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

	private final static String DAILY_DIRECTORY = "daily";

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

	private final static String DATA_SOURCE_YAHOO = "yahoo";

	private final static String DATA_SOURCE_STOOP = "stooq";

	private final static String MARKET_CANADA = "CA";

	private final static String MARKET_UNITED_STATE = "US";

	private final static String TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE = ".TO";

	private final static String TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE = ".V";

	private final static String TORONTO_STOCK_EXCHANGE = "TSX";

	private final static String TORONTO_STOCK_VENTURE_EXCHANGE = "TSXV";

	private final static String STOOQ_TICKER_SUFFIX = ".US";
	
	private final static String STOOQ_FILE_PATTERN = ".us";
	
	private final static String STRING_DASH = "-";

	private final static String STRING_EMPTY_SPACE = "";

	private final static Character CHARACTER_DASH = '-';

	private final static Character CHARACTER_DOT = '.';
	

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
		if (dataDirectory != null && dataDirectory.equals(STRING_DASH)) {
			dataDirectory = System.getProperty(USER_HOME);
		}
	}

	public List<Map<String, Object>> getSymbolsfromHistoricalDirectory(final String subDirectory) throws IOException {

		String directory = dataDirectory + File.separator + HISTORICAL_DIRECTORY;

		if (!subDirectory.equals(STRING_DASH)) {
			directory += File.separator + subDirectory;
		}

		logger.info(directory);
		File directoryPath = new File(directory);

		return Stream.of(directoryPath.listFiles()).filter(x -> !x.isDirectory() && !x.isHidden())
				.sorted((i, j) -> i.getName().compareTo(j.getName())).map(x -> {
					Map<String, Object> map = new TreeMap<>();
					String name = x.getName();
					if (name.contains("historical")) {
						map.put(mapKey.getDataSource(), DATA_SOURCE_YAHOO);
						String symbol = name.substring(0, name.length() - 17);
						map.put(mapKey.getSymbol(), symbol);
						map.put(mapKey.getTicker(), symbol);
					} else {
						map.put(mapKey.getDataSource(), DATA_SOURCE_STOOP);
						String symbol = name.substring(0, name.length() - 7);
						map.put(mapKey.getSymbol(), symbol);
						map.put(mapKey.getTicker(), symbol);
					}
					return map;
				}).collect(Collectors.toList());

	}

	public List<String> getSymbolsfromDailyDirectory(final String subDirectory) throws IOException {

		String directory = dataDirectory + File.separator + DAILY_DIRECTORY;

		if (!subDirectory.equals(STRING_DASH)) {
			directory += File.separator + subDirectory;
		}

		logger.info(directory);
		File directoryPath = new File(directory);

		return Stream.of(directoryPath.listFiles()).filter(x -> !x.isDirectory() && !x.isHidden())
				.sorted((i, j) -> i.getName().compareTo(j.getName())).map(x -> {
					return x.getName();
				}).collect(Collectors.toList());

	}

	public List<String> getSymbolsfromDetailDirectory(final String subExchange) throws IOException {

		String directory = dataDirectory + File.separator + DETAIL_DIRECTORY + File.separator + subExchange;

		logger.info(directory);
		File directoryPath = new File(directory);

		return Stream.of(directoryPath.listFiles()).filter(x -> !x.isDirectory() && !x.isHidden())
				.sorted((i, j) -> i.getName().compareTo(j.getName())).map(x -> {
					String name = x.getName();
					return name.substring(0, name.length() - 11);
				}).collect(Collectors.toList());

	}

	public Map<String, Object> parseDetailFile(final String subExchange, final String symbol) throws IOException {

		final String suffix = (subExchange.equals(TORONTO_STOCK_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE
				: (subExchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE
						: STRING_EMPTY_SPACE;
		final String market = (subExchange.equals(TORONTO_STOCK_EXCHANGE)
				|| subExchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? MARKET_CANADA : MARKET_UNITED_STATE;

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
		map.put(mapKey.getSymbol(), symbol.replaceAll(suffix, STRING_EMPTY_SPACE));
		map.put(mapKey.getSubExchange(), subExchange);
		map.put(mapKey.getMarket(), market);

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
		if (dataSource.equals(DATA_SOURCE_YAHOO)) {
			format = "yyyy-MM-dd";
		} else {
			format = "yyyyMMdd";
		}

		return format;
	}

	private List<Integer> getColumnsPosition(final String dataSource) {
		List<Integer> columns = null;
		if (dataSource.equals(DATA_SOURCE_YAHOO)) {
			columns = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
		} else {
			columns = Arrays.asList(2, 4, 5, 6, 7, 7, 8);
		}

		return columns;
	}

	private String getFullFileName(final String directory, final String subDirectory, final String fileName,
			final String dataSource, final String symbol, final String ticker) {

		String fullFileName = dataDirectory + File.separator + directory;

		if (!subDirectory.equals(STRING_DASH)) {
			fullFileName += File.separator + subDirectory;
		}
		if (!fileName.equals(STRING_DASH)) {
			fullFileName += File.separator + fileName;
		} else {
			if (dataSource.equals(DATA_SOURCE_YAHOO)) {
				fullFileName += File.separator + ticker + HISTORICAL_DAILY_FILE_SUFFIX + FILE_EXTENSION;
			} else {
				fullFileName += File.separator + symbol + STOOQ_FILE_PATTERN + FILE_EXTENSION;
			}
		}
		return fullFileName;
	}

	private int getIntervalPosition(final String dataSource) {

		if (dataSource.equals(DATA_SOURCE_STOOP)) {
			return 1;
		}
		return -1;
	}

	private int getTimePosition(final String dataSource) {

		if (dataSource.equals(DATA_SOURCE_STOOP)) {
			return 3;
		}
		return -1;
	}

	public List<Map<String, Object>> parseHistoricalFile(final String subDirectory, final String dataSource,
			final String symbol, final String ticker) throws IOException {

		final String fileName = STRING_DASH;
		List<Map<String, Object>> outputList = parseQuoteFile(HISTORICAL_DIRECTORY, subDirectory, fileName, dataSource,
				symbol, ticker);

		if (outputList.size() >= 2) {
			Map<String, Object> first = outputList.get(0);
			Map<String, Object> last = outputList.get(outputList.size() - 1);

			Map<String, Object> index = new TreeMap<String, Object>();

			index.put(mapKey.getInterval(), new String(mapValue.getDaily()));
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
		}
		return outputList;
	}

	public List<Map<String, Object>> parseDailyFile(final String subDirectory, final String fileName,
			final String dataSource) throws IOException {

		final String symbol = STRING_DASH;
		final String ticker = STRING_DASH;

		List<Map<String, Object>> outputList = parseQuoteFile(DAILY_DIRECTORY, subDirectory, fileName, dataSource,
				symbol, ticker);

		if (outputList.size() >= 0) {
			Map<String, Object> first = outputList.get(0);

			Map<String, Object> index = new TreeMap<String, Object>();

			index.put(mapKey.getTicker(), new String(ticker).toUpperCase());
			index.put(mapKey.getSymbol(), new String(symbol).toUpperCase());
			index.put(mapKey.getType(), first.get(mapKey.getType()));
			index.put(mapKey.getDate(), first.get(mapKey.getDate()));
			index.put(mapKey.getTotal(), Long.valueOf(outputList.size()));

			outputList.add(0, index);
		}
		return outputList;
	}

	private List<Map<String, Object>> parseQuoteFile(final String directory, final String subDirectory,
			final String fileName, final String dataSource, final String symbol, final String ticker)
			throws IOException {

		final List<Integer> columns = getColumnsPosition(dataSource);
		final String dateFormat = getDateFormat(dataSource);
		final int intervalPosition = getIntervalPosition(dataSource);
		final int timePosition = getTimePosition(dataSource);
		final String fullFileName = getFullFileName(directory, subDirectory, fileName, dataSource, symbol, ticker);
		logger.info(fullFileName);

		CSVFileReader csvFileReader = new CSVFileReader();

		List<String[]> list = csvFileReader.read(fullFileName, COMMA_SEPERATOR, null);

		List<Map<String, Object>> outputList = list.stream().map(x -> {
			Map<String, Object> map = null;

			Calendar calendar = getCalendar(dateFormat, x[columns.get(0)]);

			if (calendar != null) {
				int year = calendar.get(Calendar.YEAR);
				int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
				int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);

				map = new TreeMap<String, Object>();

				if (intervalPosition == -1) {
					map.put(mapKey.getInterval(), new String(mapValue.getDaily()));
				} else {
					map.put(mapKey.getInterval(), new String(x[intervalPosition]));
				}
				if (symbol.equals(STRING_DASH)) {
					String tempSymbol = x[0];
					if (tempSymbol != null && tempSymbol.endsWith(STOOQ_TICKER_SUFFIX)) {
						tempSymbol = tempSymbol.substring(0, tempSymbol.length() - 3);
					}
					map.put(mapKey.getTicker(), tempSymbol);
					map.put(mapKey.getSymbol(), tempSymbol);
				} else {
					map.put(mapKey.getTicker(), new String(ticker).toUpperCase());
					map.put(mapKey.getSymbol(), new String(symbol).toUpperCase());
				}
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

				String temp = map.get(mapKey.getTicker()).toString();
				if (temp.endsWith(TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE)
						|| temp.endsWith(TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE)) {
					map.put(mapKey.getMarket(), MARKET_CANADA);

				} else {
					map.put(mapKey.getMarket(), MARKET_UNITED_STATE);
				}
			}

			return map;
		}).filter(x -> x != null).collect(Collectors.toList());

		return outputList;
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
			logger.info(e.toString());
		}
		return calendar;
	}

	public List<Map<String, Object>> parseExchangeFile(final String exchange) throws IOException {

		String fileName = dataDirectory + File.separator + EXCHANGE_DIRECTORY + File.separator + exchange
				+ FILE_EXTENSION;

		logger.info(fileName);

		final String suffix = (exchange.equals(TORONTO_STOCK_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE
				: (exchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE
						: STRING_EMPTY_SPACE;

		final boolean neededSuffix = (exchange.equals(TORONTO_STOCK_EXCHANGE)
				|| exchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? true : false;

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
				long count = symbol.chars().filter(ch -> ch == CHARACTER_DOT).count();
				if (count == 0) {
					ticker = symbol + suffix;
				} else if (count == 1) {
					ticker = symbol.replace(CHARACTER_DOT, CHARACTER_DASH) + suffix;
				} else {
					ticker = STRING_DASH;
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