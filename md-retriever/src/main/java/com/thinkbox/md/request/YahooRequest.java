package com.thinkbox.md.request;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class YahooRequest {

	protected static final String USER_HOME = "user.home";

	protected static final String HISTORICAL_FILE_EXTENSION = "-historical.txt";
	
	protected static final String INFO_FILE_EXTENSION = "-info.txt";

	protected static final String DETAIL_FILE_EXTENSION = "-detail.txt";

	protected static final int CONNECTION_TIMEOUT = Integer
			.parseInt(System.getProperty("yahoofinance.connection.timeout", "10000"));

	protected static final String HISTQUOTES2_BASE_URL = System.getProperty("yahoofinance.baseurl.histquotes2",
			"https://query1.finance.yahoo.com/v7/finance/download/");

	protected static final String QUOTES_QUERY1V7_BASE_URL = System.getProperty("yahoofinance.baseurl.quotesquery1v7", "https://query1.finance.yahoo.com/v7/finance/quote");

	protected static final String QUOTES_QUERY2V10_BASE_URL = System.getProperty("yahoofinance.baseurl.quotesquery2v10", "https://query2.finance.yahoo.com/v10/finance/quoteSummary/");

	protected static final String UTF8_ENCODING = "UTF-8";
	
	private static final Logger log = LoggerFactory.getLogger(YahooRequest.class);

	protected String getURLParameters(Map<String, String> params) {
		StringBuilder sb = new StringBuilder();

		for (Entry<String, String> entry : params.entrySet()) {
			if (sb.length() > 0) {
				sb.append("&");
			}
			String key = entry.getKey();
			String value = entry.getValue();
			try {
				key = URLEncoder.encode(key, UTF8_ENCODING);
				value = URLEncoder.encode(value, UTF8_ENCODING);
			} catch (UnsupportedEncodingException ex) {
				log.error(ex.getMessage(), ex);
				// Still try to continue with unencoded values
			}
			sb.append(String.format("%s=%s", key, value));
		}
		return sb.toString();
	}

}
