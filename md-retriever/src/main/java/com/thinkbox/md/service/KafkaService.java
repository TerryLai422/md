package com.thinkbox.md.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.thinkbox.md.request.YahooHistoricalRequest;
import com.thinkbox.md.request.YahooInfoRequest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

@Service
public class KafkaService {

	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	public void publish(String topic, Map<String, Object> map) {
		logger.info(String.format("Message sent -> %s", map));
		kafkaTemplate.send(topic, map);
	}
	@Async("asyncExecutor")
	@KafkaListener(topics = "retrieve.info.data", containerFactory = "mapListener")
	public void processInfo(Map<String, Object> map) {
		logger.info("receive topic: retrieve.info.data - map: {}", map);
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
		}
	}

	@Async("asyncExecutor")
	@KafkaListener(topics = "retrieve.historical.data", containerFactory = "mapListener")
	public void processHistorical(Map<String, Object> map) {
		logger.info("receive topic: retrieve.historical.data - map: {}", map);
		String symbol = map.getOrDefault("symbol", "-").toString();
		String date = map.getOrDefault("date", "-").toString();

		if (!symbol.equals("-")) {
			if (!date.equals("-")) {
				Calendar startDate = null;
				Date sDate;
				try {
					sDate = new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(date);
					startDate = Calendar.getInstance();
					startDate.setTime(sDate);
				} catch (ParseException e) {
					logger.info(e.toString());
				}

				if (startDate != null) {
					YahooHistoricalRequest yahooHistoricalRequest = new YahooHistoricalRequest(symbol, startDate);
					try {
						yahooHistoricalRequest.get();
						// TODO publish("process.data.historical", map);
						logger.info("Finished retrieve historical data - map: {}", map);
					} catch (IOException e) {
						logger.info(e.toString());
					}
				}
			} else {
				YahooHistoricalRequest yahooHistoricalRequest = new YahooHistoricalRequest(symbol);
				try {
					yahooHistoricalRequest.get();
					// TODO publish("process.data.historical", map);
					logger.info("Finished retrieve historical data - map: {}", map);
				} catch (IOException e) {
					logger.info(e.toString());
				}
			}
		}
	}

}