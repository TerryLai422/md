package com.thinkbox.md.service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.thinkbox.md.request.YahooHistoricalRequest;
import com.thinkbox.md.request.YahooInfoRequest;

@Component
public class RetrieverService {

	private final Logger logger = LoggerFactory.getLogger(RetrieverService.class);

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	public void processInfo(Map<String, Object> map) {
		String symbol = map.getOrDefault("symbol", "-").toString();

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
	
	public void processHistorical(Map<String, Object> map) {
		String symbol = map.getOrDefault("symbol", "-").toString();
		String date = map.getOrDefault("date", "-").toString();
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
}
