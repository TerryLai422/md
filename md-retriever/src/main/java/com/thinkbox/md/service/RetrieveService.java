package com.thinkbox.md.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.request.YahooDetailRequest;
import com.thinkbox.md.request.YahooHistoricalRequest;
import com.thinkbox.md.request.YahooInfoRequest;
import com.thinkbox.md.request.YahooRequest;

@Component
public class RetrieveService {

	private final Logger logger = LoggerFactory.getLogger(RetrieveService.class);

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	@Autowired
	private MapKeyParameter mapKey;

	private YahooRequest getYahooRequest(Map<String, Object> map) {
		YahooRequest yahooRequest = null;

		String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
		String yahooType = map.getOrDefault(mapKey.getYahooType(), "-").toString();

		if (yahooType.equals("-")) {
			logger.info("Request missing yahooType");
			return yahooRequest;

		}
		if (ticker.equals("-")) {
			logger.info("Request missing ticker");
			return yahooRequest;
		}

		if (yahooType.equals("info")) {
			yahooRequest = new YahooInfoRequest(ticker);
		} else if (yahooType.equals("detail")) {
			yahooRequest = new YahooDetailRequest(ticker);
		} else if (yahooType.equals("historical")) {
			String date = map.getOrDefault(mapKey.getDate(), "-").toString();
			String interval = map.getOrDefault("interval", "-").toString();

			Calendar startDate = null;
			if (!date.equals("-")) {
				Date sDate;
				try {
					sDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
					startDate = Calendar.getInstance();
					startDate.setTime(sDate);
				} catch (ParseException e) {
					logger.info(e.toString());
				}
			}
			if (startDate != null) {
				yahooRequest = new YahooHistoricalRequest(ticker, interval, startDate);
			} else {
				yahooRequest = new YahooHistoricalRequest(ticker, interval);
			}
		}

		return yahooRequest;
	}
	
	public void retrieveYahoo(Map<String, Object> map) {

		YahooRequest yahooRequest = getYahooRequest(map);

		if (yahooRequest != null) {
			try {
				yahooRequest.get();
				logger.info("Finished retrieve historical data - map: {}", map);
			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Cannot create YahooRquest: {}", map.toString());
		}
	}

	public List<Map<String, Object>> retrieveYahooList(List<Map<String, Object>> list) {
		List<Map<String, Object>> outputList = Arrays.asList();

		final Map<String, Object> firstMap = list.get(0);
		final int wait = Integer.valueOf(firstMap.get(mapKey.getWait()).toString());
		final String from = firstMap.getOrDefault(mapKey.getFrom(), "-").toString();
		final String yahooType = firstMap.getOrDefault(mapKey.getYahooType(), "-").toString();
		final String key = (yahooType.equals("historical")) ? "retrieveHistorical"
				: (yahooType.equals("detail")) ? "retrieveDetail" : (yahooType.equals("info")) ? "retrieveInfo" : "-";

		if (key.equals("-")) {
			logger.info("Skip retrieve Yahoo data (missing/incorrect yahooType parameter");
		} else {
			Stream<Map<String, Object>> intermedicateList = null;
			if (from.equals("-")) {
				intermedicateList = list.stream().skip(1).sorted(
						(i, j) -> i.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()))
						.limit(4);
			} else {
				intermedicateList = list.stream().skip(1)
						.filter(x -> x.get(mapKey.getTicker()).toString().compareTo(from) > 1).sorted((i, j) -> i
								.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()))
						.limit(4);
			}

			outputList = intermedicateList.map(map -> {
				logger.info("Start retrieve Yahoo data - map: {}", map);

				String ticker = map.get(mapKey.getTicker()).toString();
				Map<String, Object> hMap = new TreeMap<>();
				firstMap.forEach((x, y) -> {
					hMap.put(x, y);
				});
				hMap.put(mapKey.getTicker(), ticker);
				YahooRequest yahooRequest = getYahooRequest(hMap);
				try {
					yahooRequest.get();
					logger.info("Finished retrieve info data - map: {}", map);
					map.put(key, true);
				} catch (IOException e) {
					logger.info(e.toString());
					map.put(key, false);
				}

				try {
					System.out.println("Sleep :" + wait);
					Thread.sleep(wait);
				} catch (InterruptedException e) {
					logger.info(e.toString());
				}
				return map;
			}).toList();

		}
		return outputList;
	}

}
