package com.thinkbox.md.service;

import java.io.IOException;
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;
import com.thinkbox.md.request.YahooDetailRequest;
import com.thinkbox.md.request.YahooHistoricalRequest;
import com.thinkbox.md.request.YahooInfoRequest;
import com.thinkbox.md.request.YahooRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RetrieveService {

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	private final static String DEFAULT_STRING_VALUE = "-";

	@Autowired
	private MapValueParameter mapValue;

	@Autowired
	private MapKeyParameter mapKey;

	private YahooRequest getYahooRequest(Map<String, Object> map) {
		YahooRequest yahooRequest = null;

		String ticker = map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE).toString();
		String dataType = map.getOrDefault(mapKey.getDataType(), DEFAULT_STRING_VALUE).toString();

		if (dataType.equals(DEFAULT_STRING_VALUE)) {
			log.info("Request missing yahooType");
			return yahooRequest;

		}
		if (ticker.equals(DEFAULT_STRING_VALUE)) {
			log.info("Request missing ticker");
			return yahooRequest;
		}

		if (dataType.equals(mapValue.getInfo())) {
			yahooRequest = new YahooInfoRequest(ticker);
		} else if (dataType.equals(mapValue.getDetail())) {
			yahooRequest = new YahooDetailRequest(ticker);
		} else if (dataType.equals(mapValue.getHistorical())) {
			String date = map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();
			String interval = map.getOrDefault(mapKey.getInterval(), DEFAULT_STRING_VALUE).toString();

			Calendar startDate = null;
			if (!date.equals(DEFAULT_STRING_VALUE)) {
				Date sDate;
				try {
					sDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
					startDate = Calendar.getInstance();
					startDate.setTime(sDate);
				} catch (ParseException e) {
					log.info(e.toString());
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
				log.info("Finished retrieve historical data - map: {}", map);
			} catch (IOException e) {
				log.info(e.toString());
			}
		} else {
			log.info("Cannot create YahooRquest: {}", map.toString());
		}
	}

	public List<Map<String, Object>> retrieveYahooList(List<Map<String, Object>> list) {
		List<Map<String, Object>> outputList = Arrays.asList();

		final Map<String, Object> firstMap = list.get(0);
		final int wait = Integer.valueOf(firstMap.get(mapKey.getWait()).toString());
		final String from = firstMap.getOrDefault(mapKey.getFrom(), DEFAULT_STRING_VALUE).toString();
		final String dataType = firstMap.getOrDefault(mapKey.getDataType(), DEFAULT_STRING_VALUE).toString();
		final String key = (dataType.equals(mapValue.getHistorical())) ? "retrieveHistorical"
				: (dataType.equals(mapValue.getDetail())) ? "retrieveDetail" : (dataType.equals(mapValue.getInfo())) ? "retrieveInfo" : DEFAULT_STRING_VALUE;

		if (key.equals(DEFAULT_STRING_VALUE)) {
			log.info("Skip retrieve Yahoo data (missing/incorrect yahooType parameter");
		} else {
			Stream<Map<String, Object>> intermedicateList = null;
			if (from.equals(DEFAULT_STRING_VALUE)) {
				intermedicateList = list.stream().skip(1).sorted(
						(i, j) -> i.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
			} else {
				intermedicateList = list.stream().skip(1)
						.filter(x -> x.get(mapKey.getTicker()).toString().compareTo(from) > 1).sorted((i, j) -> i
								.get(mapKey.getTicker()).toString().compareTo(j.get(mapKey.getTicker()).toString()));
			}

			outputList = intermedicateList.map(map -> {
				log.info("Start retrieve Yahoo data - map: {}", map);

				String ticker = map.get(mapKey.getTicker()).toString();
				Map<String, Object> hMap = new TreeMap<>();
				firstMap.forEach((x, y) -> {
					hMap.put(x, y);
				});
				hMap.put(mapKey.getTicker(), ticker);
				YahooRequest yahooRequest = getYahooRequest(hMap);
				try {
					yahooRequest.get();
					log.info("Finished retrieve info data - map: {}", map);
					map.put(key, true);
				} catch (IOException e) {
					log.info(e.toString());
					map.put(key, false);
				}

				try {
					System.out.println("Sleep :" + wait);
					Thread.sleep(wait);
				} catch (InterruptedException e) {
					log.info(e.toString());
				}
				return map;
			}).collect(Collectors.toList());

		}
		return outputList;
	}
}