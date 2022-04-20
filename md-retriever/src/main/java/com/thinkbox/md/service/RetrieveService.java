package com.thinkbox.md.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

	public void retrieveInfo(Map<String, Object> map) {
		String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();

		if (!ticker.equals("-")) {
			YahooInfoRequest yahooInfoRequest = new YahooInfoRequest(ticker);
			try {
				yahooInfoRequest.get();
				logger.info("Finished retrieve info data - map: {}", map);

			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Request missing ticker");
		}
	}

	public void retrieveDetail(Map<String, Object> map) {
		String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();

		if (!ticker.equals("-")) {
			YahooDetailRequest yahooDetailRequest = new YahooDetailRequest(ticker);
			try {
				yahooDetailRequest.get();
				logger.info("Finished retrieve info data - map: {}", map);

			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Request missing ticker");
		}
	}

	private YahooHistoricalRequest getYahooHistoricalRequest(Map<String, Object> map) {
		YahooHistoricalRequest yahooHistoricalRequest = null;

		String ticker = map.getOrDefault(mapKey.getTicker(), "-").toString();
		String date = map.getOrDefault(mapKey.getDate(), "-").toString();
		String interval = map.getOrDefault("interval", "-").toString();
System.out.println("Date:" + date);
		if (!ticker.equals("-")) {
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
				yahooHistoricalRequest = new YahooHistoricalRequest(ticker, interval, startDate);
			} else {
				yahooHistoricalRequest = new YahooHistoricalRequest(ticker, interval);
			}
		} else {
			logger.info("Request missing ticker");
		}

		return yahooHistoricalRequest;
	}

	public void retrieveHistorical(Map<String, Object> map) {

		YahooHistoricalRequest yahooHistoricalRequest = getYahooHistoricalRequest(map);

		if (yahooHistoricalRequest != null) {
			try {
				yahooHistoricalRequest.get();
				logger.info("Finished retrieve historical data - map: {}", map);
			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Cannot create YahooHistoricalRquest: {}", map.toString());
		}
	}

	public List<Map<String, Object>> retrieveInfoList(List<Map<String, Object>> list) {
		Map<String, Object> firstMap = list.get(0);

		final int wait = Integer.valueOf(firstMap.get(mapKey.getWait()).toString());
		final String from = firstMap.getOrDefault("from", "-").toString();

		Stream<Map<String, Object>> intermedicateList = null;
		if (from.equals("-")) {
			intermedicateList = list.stream().skip(1).sorted(
					(i, j) -> i.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
		} else {
			intermedicateList = list.stream().skip(1)
					.filter(x -> x.get(mapKey.getTicker()).toString().compareTo(from) > 1).sorted((i, j) -> i
							.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
		}

		intermedicateList.forEach(map -> {
			logger.info("Start retrieve info data - map: {}", map);

			String ticker = map.get(mapKey.getTicker()).toString();
			if (!ticker.equals("-")) {
				YahooInfoRequest yahooInfoRequest = new YahooInfoRequest(ticker);
				try {
					yahooInfoRequest.get();
					logger.info("Finished retrieve info data - map: {}", map);
					map.put("RetrieveInfo", true);
				} catch (IOException e) {
					logger.info(e.toString());
					map.put("RetrieveInfo", false);
				}

				try {
					System.out.println("Sleep :" + wait);
					Thread.sleep(wait);
				} catch (InterruptedException e) {
					logger.info(e.toString());
				}
			} else {
				logger.info("Skip retrieve info data - map: {}", map);
			}

		});

		List<Map<String, Object>> outputList = list;

		return outputList;
	}

	public List<Map<String, Object>> retrieveYahooList(List<Map<String, Object>> list) {
		final Map<String, Object> firstMap = list.get(0);
		System.out.println("First: " + firstMap);
		final int wait = Integer.valueOf(firstMap.get(mapKey.getWait()).toString());
		final String from = firstMap.getOrDefault(mapKey.getFrom(), "-").toString();
		final String yahooType = firstMap.getOrDefault(mapKey.getYahooType(), "-").toString();

		if (yahooType.equals("-")) {
			logger.info("Skip retrieve Yahoo data (missing yahooType parameter");
		} else {
			Stream<Map<String, Object>> intermedicateList = null;
			if (from.equals("-")) {
				intermedicateList = list.stream().skip(1).sorted(
						(i, j) -> i.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
			} else {
				intermedicateList = list.stream().skip(1)
						.filter(x -> x.get(mapKey.getTicker()).toString().compareTo(from) > 1).sorted((i, j) -> i
								.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
			}

			intermedicateList.forEach(map -> {
				logger.info("Start retrieve Yahoo data - map: {}", map);

				String ticker = map.get(mapKey.getTicker()).toString();
				if (!ticker.equals("-")) {
					YahooRequest yahooRequest = null;
					if (yahooType.equals("historical")) {
						Map<String, Object> hMap = new TreeMap<>();
						firstMap.forEach((x, y) -> {
							hMap.put(x, y);
						});
						hMap.put(mapKey.getTicker(), ticker);
						yahooRequest = getYahooHistoricalRequest(hMap);
					} else {
						yahooRequest = new YahooDetailRequest(ticker);
					}
					try {
						yahooRequest.get();
						logger.info("Finished retrieve info data - map: {}", map);
					} catch (IOException e) {
						logger.info(e.toString());
					}

					try {
						System.out.println("Sleep :" + wait);
						Thread.sleep(wait);
					} catch (InterruptedException e) {
						logger.info(e.toString());
					}
				} else {
					logger.info("Skip retrieve info data - map: {}", map);
				}

			});

			return list;
		}
		return new ArrayList<Map<String, Object>>();
	}

}
