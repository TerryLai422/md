package com.thinkbox.md.service;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.thinkbox.md.util.CSVFileReader;

@Component
public class FileParserService {

	private final Logger logger = LoggerFactory.getLogger(FileParserService.class);

	private final static String USER_HOME = "user.home";

	private final static String HISTORICAL_DIRECTORY = "historical";

	private final static String EXCHANGE_DIRECTORY = "exchange";

	private final static String HISTORICAL_DAILY_FILE_POSTFIX = "-d-historical";

	private final static String FILE_EXTENSION = ".txt";

	private final static Character COMMA_SEPERATOR = ',';

	private final static Character TAB_SEPERATOR = '\t';

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	@Value("${app.data.directory:-}")
	private String dataDirectory;

	@PostConstruct
	public void init() {
		if (dataDirectory != null && dataDirectory.equals("-")) {
			dataDirectory = System.getProperty(USER_HOME);
		}
	}

	public List<Map<String, Object>> parseHistoricalFile(final String symbol) throws IOException {

		String fileName = dataDirectory + File.separator + HISTORICAL_DIRECTORY + File.separator + symbol
				+ HISTORICAL_DAILY_FILE_POSTFIX + FILE_EXTENSION;

		logger.info(fileName);

		CSVFileReader csvFileReader = new CSVFileReader();

		List<String[]> list = csvFileReader.read(fileName, COMMA_SEPERATOR, null);

		List<Map<String, Object>> mapList = list.stream().map(x -> {
			Map<String, Object> map = null;

			try {
				Calendar calendar = null;
				String date = x[0];
				Date sDate;
				sDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
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

				map.put("type", new String("daily"));
				map.put("symbol", new String(symbol));
				map.put("date", new String(x[0]));
				map.put("year", year);
				map.put("month", calendar.get(Calendar.MONTH) + 1);
				map.put("day", calendar.get(Calendar.DATE));
				map.put("dayOfYear", dayOfYear);
				map.put("weekOfYear", weekOfYear);
				map.put("dayOfWeek", calendar.get(Calendar.DAY_OF_WEEK));
				if (weekOfYear == 1 && ((year % 4 != 0 && dayOfYear >= 362)
						|| (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
								&& dayOfYear >= 363))) {
					map.put("yearForWeek", year + 1);
				} else {
					map.put("yearForWeek", year);
				}
				map.put("open", Double.parseDouble(x[1]));
				map.put("high", Double.parseDouble(x[2]));
				map.put("low", Double.parseDouble(x[3]));
				map.put("close", Double.parseDouble(x[4]));
				map.put("adjClose", Double.parseDouble(x[5]));
				map.put("volume", Long.parseLong(x[6]));

			} catch (ParseException e) {
				logger.info(e.toString());
			}
			return map;
		}).filter(x -> x != null).collect(Collectors.toList());

		Map<String, Object> first = mapList.get(0);
		Map<String, Object> last = mapList.get(mapList.size() - 1);

		Map<String, Object> index = new TreeMap<String, Object>();

		index.put("type", new String("daily"));
		index.put("symbol", new String(symbol));
		index.put("from", first.get("date"));
		index.put("fromYear", first.get("year"));
		index.put("fromMonth", first.get("month"));
		index.put("fromDay", first.get("day"));
		index.put("fromWeekOfYear", first.get("weekOfYear"));
		index.put("fromDayOfWeek", first.get("dayOfWeek"));

		index.put("to", last.get("date"));
		index.put("toYear", last.get("year"));
		index.put("toMonth", last.get("month"));
		index.put("toDay", last.get("day"));
		index.put("toWeekOfYear", last.get("weekOfYear"));
		index.put("toDayOfWeek", last.get("dayOfWeek"));

		index.put("total", Long.valueOf(mapList.size()));
		mapList.add(0, index);

		return mapList;
	}

	public List<Map<String, Object>> parseExchangeFile(String exchange) throws IOException {

		String fileName = dataDirectory + File.separator + EXCHANGE_DIRECTORY + File.separator + exchange
				+ FILE_EXTENSION;

		logger.info(fileName);

		CSVFileReader csvFileReader = new CSVFileReader();

		List<String[]> list = csvFileReader.read(fileName, TAB_SEPERATOR, null);

		List<Map<String, Object>> mapList = list.stream().map(x -> {
			Map<String, Object> map = new TreeMap<String, Object>();
			map.put("symbol", x[0]);
			map.put("name", x[1]);
			return map;
		}).collect(Collectors.toList());

		Map<String, Object> index = new TreeMap<String, Object>();
		index.put("exchange", new String(exchange));
		index.put("total", Long.valueOf(mapList.size()));
		mapList.add(0, index);

		return mapList;
	}

}