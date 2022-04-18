package com.thinkbox.md.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YahooDetailRequest extends YahooRequest {

    private static final Logger log = LoggerFactory.getLogger(YahooDetailRequest.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected final String symbol;

    public YahooDetailRequest(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public void get() throws IOException {

        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("symbols", this.symbol);
        
        String url = QUOTES_QUERY2V10_BASE_URL + symbol + "?modules=" + getModules() + "&ssl=true";
        
        // Get JSON from Yahoo
        log.info("Sending request: " + url);

        URL request = new URL(url);
        RedirectableRequest redirectableRequest = new RedirectableRequest(request, 5);
        redirectableRequest.setConnectTimeout(CONNECTION_TIMEOUT);
        redirectableRequest.setReadTimeout(CONNECTION_TIMEOUT);
        URLConnection connection = redirectableRequest.openConnection();

        InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
        JsonNode node = objectMapper.readTree(inputStreamReader);
//        log.info("Node: {}", node.toPrettyString());
        if(node.has("quoteSummary") && node.get("quoteSummary").has("result")) {
            node = node.get("quoteSummary").get("result");
            if (node.size() > 0) {
            	for(int i = 0; i < node.size(); i++) {
            		saveAsFile(symbol, node.get(i));
            	}
            } else {
            	throw new IOException("Can't find detail for symbol: [ " + symbol + " ]");
            }
        } else {
            throw new IOException("Invalid response");
        }
    }
    
    private String getModules() throws UnsupportedEncodingException {
    	List<String> list = Arrays.asList("defaultKeyStatistics","price","summaryProfile");
    	String hello = list.toString().substring(1, list.toString().length() - 1).replaceAll("\\s+", "");
    	return URLEncoder.encode(list.toString().substring(1, list.toString().length() - 1).replaceAll("\\s+", ""), UTF8_ENCODING);
    }
    
    private void saveAsFile(String symbol, JsonNode node) throws IOException {
 		String homeDirectory = System.getProperty(USER_HOME);
		String fullFilePath = homeDirectory + File.separator + "detail" + File.separator + symbol + DETAIL_FILE_EXTENSION;

        FileWriter file = new FileWriter(fullFilePath);
        file.write(node.toPrettyString());
        file.close();
    }
}
