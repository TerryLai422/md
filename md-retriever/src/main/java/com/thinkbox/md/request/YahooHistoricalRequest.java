package com.thinkbox.md.request;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.StandardCopyOption;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YahooHistoricalRequest extends YahooRequest {

	public static final String INTERVAL_DAILY = "1d";

	public static final String INTERVAL_WEEKLY = "5d";

	public static final String INTERVAL_MONTHLY = "1mo";

	public static final Calendar DEFAULT_FROM = Calendar.getInstance();

	public static final Calendar DEFAULT_TO = Calendar.getInstance();

	public static final String DEFAULT_INTERVAL = INTERVAL_DAILY;

	static {
		DEFAULT_FROM.add(Calendar.YEAR, -1);
	}

	private static final Logger log = LoggerFactory.getLogger(YahooHistoricalRequest.class);

	private final String symbol;

	private final Calendar from;

	private final Calendar to;

	private final String interval;

	private final boolean displayOnly;

	public YahooHistoricalRequest(String symbol) {
		this(symbol, DEFAULT_INTERVAL);
	}

	public YahooHistoricalRequest(String symbol, Calendar from) {
		this(symbol, from, DEFAULT_TO, DEFAULT_INTERVAL, false);
	}

	public YahooHistoricalRequest(String symbol, String interval) {
		this(symbol, DEFAULT_FROM, DEFAULT_TO, interval, false);
	}

	public YahooHistoricalRequest(String symbol, Calendar from, Calendar to) {
		this(symbol, from, to, DEFAULT_INTERVAL, false);
	}

	public YahooHistoricalRequest(String symbol, Calendar from, Calendar to, String interval, boolean displayOnly) {
		this.symbol = symbol;
		this.from = this.cleanHistCalendar(from);
		this.to = this.cleanHistCalendar(to);
		this.interval = interval;
		this.displayOnly = displayOnly;
	}

	public YahooHistoricalRequest(String symbol, Date from, Date to) {
		this(symbol, from, to, DEFAULT_INTERVAL);
	}

	public YahooHistoricalRequest(String symbol, Date from, Date to, String interval) {
		this(symbol, interval);
		this.from.setTime(from);
		this.to.setTime(to);
		this.cleanHistCalendar(this.from);
		this.cleanHistCalendar(this.to);
	}

	private Calendar cleanHistCalendar(Calendar cal) {
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.HOUR, 0);
		return cal;
	}

	public void get() throws IOException {

		if (this.from.after(this.to)) {
			log.warn("Unable to retrieve historical quotes. " + "From-date should not be after to-date. From: "
					+ this.from.getTime() + ", to: " + this.to.getTime());
			return;
		}

		Map<String, String> params = new LinkedHashMap<String, String>();
		params.put("period1", String.valueOf(this.from.getTimeInMillis() / 1000));
		params.put("period2", String.valueOf(this.to.getTimeInMillis() / 1000));
		params.put("interval", this.interval);
		params.put("crumb", CrumbManager.getCrumb());

		String url = HISTQUOTES2_BASE_URL + URLEncoder.encode(this.symbol, UTF8_ENCODING) + "?" + getURLParameters(params);

		log.info("Sending request: " + url);

		URL request = new URL(url);
		RedirectableRequest redirectableRequest = new RedirectableRequest(request, 5);
		redirectableRequest.setConnectTimeout(CONNECTION_TIMEOUT);
		redirectableRequest.setReadTimeout(CONNECTION_TIMEOUT);
		Map<String, String> requestProperties = new HashMap<String, String>();
		requestProperties.put("Cookie", CrumbManager.getCookie());
		URLConnection connection = redirectableRequest.openConnection(requestProperties);

		if (displayOnly) {
			display(connection.getInputStream());
			throw new IOException("Display only for symbol: [ " + symbol + " ]");
		} else {
			saveAsFile(connection.getInputStream(), symbol, HISTORICAL_FILE_EXTENSION);
		}
	}

	private void saveAsFile(InputStream inputStream, String fileName, String fileExtension) throws IOException {
		String homeDirectory = System.getProperty(USER_HOME);
		String fullFilePath = homeDirectory + File.separator + "historical" + File.separator + fileName + fileExtension;

		File targetFile = new File(fullFilePath);

		java.nio.file.Files.copy(inputStream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	private void display(InputStream inputStream) throws IOException {
		InputStreamReader is = new InputStreamReader(inputStream);
		BufferedReader br = new BufferedReader(is);
		br.readLine(); // skip the first line
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			log.info("Parsing CSV line: " + unescape(line));
		}
	}

	private String unescape(String data) {
		StringBuilder buffer = new StringBuilder(data.length());
		for (int i = 0; i < data.length(); i++) {
			if ((int) data.charAt(i) > 256) {
				buffer.append("\\u").append(Integer.toHexString((int) data.charAt(i)));
			} else {
				if (data.charAt(i) == '\n') {
					buffer.append("\\n");
				} else if (data.charAt(i) == '\t') {
					buffer.append("\\t");
				} else if (data.charAt(i) == '\r') {
					buffer.append("\\r");
				} else if (data.charAt(i) == '\b') {
					buffer.append("\\b");
				} else if (data.charAt(i) == '\f') {
					buffer.append("\\f");
				} else if (data.charAt(i) == '\'') {
					buffer.append("\\'");
				} else if (data.charAt(i) == '\"') {
					buffer.append("\\\"");
				} else if (data.charAt(i) == '\\') {
					buffer.append("\\\\");
				} else {
					buffer.append(data.charAt(i));
				}
			}
		}
		return buffer.toString();
	}

}
