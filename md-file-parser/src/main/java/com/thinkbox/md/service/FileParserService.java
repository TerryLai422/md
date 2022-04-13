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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;
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

	@Autowired
	private MapKeyParameter mapKey;
	
	@Autowired
	private MapValueParameter mapValue;
	
	
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

				map.put(mapKey.getType(), new String(mapValue.getDaily()));
				map.put(mapKey.getSymbol(), new String(symbol));
				map.put(mapKey.getDate(), new String(x[0]));
				map.put(mapKey.getYear(), year);
				map.put(mapKey.getMonth(), calendar.get(Calendar.MONTH) + 1);
				map.put(mapKey.getDay(), calendar.get(Calendar.DATE));
				map.put(mapKey.getDayOfYear(), dayOfYear);
				map.put(mapKey.getWeekOfYear(), weekOfYear);
				map.put(mapKey.getDayOfWeek(), calendar.get(Calendar.DAY_OF_WEEK));
				if (weekOfYear == 1 && ((year % 4 != 0 && dayOfYear >= 362)
						|| (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
								&& dayOfYear >= 363))) {
					map.put(mapKey.getYearForWeek(), year + 1);
				} else {
					map.put(mapKey.getYearForWeek(), year);
				}
				map.put(mapKey.getOpen(), Double.parseDouble(x[1]));
				map.put(mapKey.getHigh(), Double.parseDouble(x[2]));
				map.put(mapKey.getLow(), Double.parseDouble(x[3]));
				map.put(mapKey.getClose(), Double.parseDouble(x[4]));
				map.put(mapKey.getAdjClose(), Double.parseDouble(x[5]));
				map.put(mapKey.getVolume(), Long.parseLong(x[6]));

			} catch (ParseException e) {
				logger.info(e.toString());
			}
			return map;
		}).filter(x -> x != null).collect(Collectors.toList());

		Map<String, Object> first = mapList.get(0);
		Map<String, Object> last = mapList.get(mapList.size() - 1);

		Map<String, Object> index = new TreeMap<String, Object>();

		index.put(mapKey.getType(), new String(mapValue.getDaily()));
		index.put(mapKey.getSymbol(), new String(symbol));
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

		index.put(mapKey.getTotal(), Long.valueOf(mapList.size()));
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
			map.put(mapKey.getSymbol(), x[0]);
			map.put("name", x[1]);
			return map;
		}).collect(Collectors.toList());

		Map<String, Object> index = new TreeMap<String, Object>();
		index.put("exchange", new String(exchange));
		index.put(mapKey.getTotal(), Long.valueOf(mapList.size()));
		mapList.add(0, index);

		return mapList;
	}

}