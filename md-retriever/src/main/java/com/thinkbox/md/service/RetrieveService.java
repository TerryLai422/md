package com.thinkbox.md.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.request.YahooDetailRequest;
import com.thinkbox.md.request.YahooHistoricalRequest;
import com.thinkbox.md.request.YahooInfoRequest;

@Component
public class RetrieveService {

	private final Logger logger = LoggerFactory.getLogger(RetrieveService.class);

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	@Autowired
	private MapKeyParameter mapKey;

	
	public void retrieveInfo(Map<String, Object> map) {
		String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();

		if (!symbol.equals("-")) {
			YahooInfoRequest yahooInfoRequest = new YahooInfoRequest(symbol);
			try {
				yahooInfoRequest.get();
				// TODO publish("process.data.info", map);
				logger.info("Finished retrieve info data - map: {}", map);

			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Request missing symbol");
		}
	}
	
	public void retrieveDetail(Map<String, Object> map) {
		String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();

		if (!symbol.equals("-")) {
			YahooDetailRequest yahooDetailRequest = new YahooDetailRequest(symbol);
			try {
				yahooDetailRequest.get();
				// TODO publish("process.data.info", map);
				logger.info("Finished retrieve info data - map: {}", map);

			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Request missing symbol");
		}
	}
	
	public void retrieveHistorical(Map<String, Object> map) {
		String symbol = map.getOrDefault(mapKey.getSymbol(), "-").toString();
		String date = map.getOrDefault(mapKey.getDate(), "-").toString();
		String interval = map.getOrDefault("interval", "-").toString();

		if (!symbol.equals("-")) {
			YahooHistoricalRequest yahooHistoricalRequest = null;
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
				yahooHistoricalRequest = new YahooHistoricalRequest(symbol, interval, startDate);
			} else {
				yahooHistoricalRequest = new YahooHistoricalRequest(symbol, interval);
			}

			try {
				yahooHistoricalRequest.get();
				// TODO publish("process.data.historical", map);
				logger.info("Finished retrieve historical data - map: {}", map);
			} catch (IOException e) {
				logger.info(e.toString());
			}
		} else {
			logger.info("Request missing symbol");
		}
	}

	public List<Map<String, Object>> retrieveInfoList(List<Map<String, Object>> list) {
		Map<String, Object> first = list.get(0);
		System.out.println("First: " + first);
		final int wait = Integer.valueOf(first.get(mapKey.getWait()).toString());
		final String from = first.getOrDefault("from", "-").toString();

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

	public List<Map<String, Object>> retrieveDetailList(List<Map<String, Object>> list) {
		Map<String, Object> first = list.get(0);
		System.out.println("First: " + first);
		final int wait = Integer.valueOf(first.get(mapKey.getWait()).toString());
		final String from = first.getOrDefault("from", "-").toString();

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
				YahooDetailRequest yahooDetailRequest = new YahooDetailRequest(ticker);
				try {
					yahooDetailRequest.get();
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

}
