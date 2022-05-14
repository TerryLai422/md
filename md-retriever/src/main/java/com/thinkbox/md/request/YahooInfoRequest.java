package com.thinkbox.md.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashMap;
import java.util.Map;

public class YahooInfoRequest extends YahooRequest {

	private static final Logger log = LoggerFactory.getLogger(YahooInfoRequest.class);

	private static final ObjectMapper objectMapper = new ObjectMapper();

	protected final String symbols;

	public YahooInfoRequest(String symbols) {
		this.symbols = symbols;
	}

	public String getSymbols() {
		return symbols;
	}

	public void get() throws IOException {

		Map<String, String> params = new LinkedHashMap<String, String>();
		params.put("symbols", this.symbols);

		String url = QUOTES_QUERY1V7_BASE_URL + "?" + getURLParameters(params);

		// Get JSON from Yahoo
		log.info("Sending request: " + url);

		URL request = new URL(url);
		RedirectableRequest redirectableRequest = new RedirectableRequest(request, 5);
		redirectableRequest.setConnectTimeout(CONNECTION_TIMEOUT);
		redirectableRequest.setReadTimeout(CONNECTION_TIMEOUT);
		URLConnection connection = redirectableRequest.openConnection();

		try (InputStream inputStream = connection.getInputStream();
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
			JsonNode node = objectMapper.readTree(inputStreamReader);
//        log.info("Node: {}", node.toPrettyString());
			if (node.has("quoteResponse") && node.get("quoteResponse").has("result")) {
				node = node.get("quoteResponse").get("result");
				if (node.size() > 0) {
					for (int i = 0; i < node.size(); i++) {
						saveAsFile(node.get(i));
					}
				} else {
					throw new IOException("Can't find info for symbols: [ " + symbols + " ]");
				}
			} else {
				throw new IOException("Invalid response");
			}
		}
	}

	private void saveAsFile(JsonNode node) throws IOException {
		String symbol = node.get("symbol").asText();
		String homeDirectory = System.getProperty(USER_HOME);
		String fullFilePath = homeDirectory + File.separator + "info" + File.separator + symbol + INFO_FILE_EXTENSION;

		try (FileWriter fileWriter = new FileWriter(fullFilePath)) {
			fileWriter.write(node.toPrettyString());
		}
	}
}
