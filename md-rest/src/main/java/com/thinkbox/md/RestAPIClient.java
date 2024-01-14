package com.thinkbox.md;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders; 
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RestAPIClient {

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private ResourceLoader resourceLoader;

	@Autowired
	private RestAPIProperties restProperties;

	public Map<String, Object> send(Map<String, String> inMap, String serviceName) {
		String STRING_MESSAGE = " service name is not defined in the configuration file.";
		if (log.isInfoEnabled()) {
			log.info("Sending service: {}", serviceName);
		}
		final Map<String, Object> serviceMap = restProperties.getServices().get(serviceName);
		if (serviceMap == null) {
			Map<String, Object> serviceResponse = new HashMap<>();
			serviceResponse.put("httpStatus", HttpStatus.NOT_FOUND);
			serviceResponse.put("message", serviceName + STRING_MESSAGE);
			return serviceResponse;
		}
		return getResponse(serviceMap, inMap);
	}

	private void displayResponseEntity(ResponseEntity<String> responseEntity) {
		if (log.isDebugEnabled()) {
			log.debug("Http Status: {}", responseEntity.getStatusCode());
			log.debug("Http Headers: {}", responseEntity.getHeaders());
			log.debug("Http Body: {}", responseEntity.getBody());
		}
	}

	private Map<String, Object> getResponse(Map<String, Object> serviceMap, Map<String, String> inMap) {
		ResponseEntity<String> responseEntity = getResponseEntity(serviceMap, inMap);
		displayResponseEntity(responseEntity);

		return getResponse(serviceMap, responseEntity);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getResponse(Map<String, Object> serviceMap, ResponseEntity<String> responseEntity) {
		Map<String, Object> serviceResponse = new HashMap<>();
		HttpStatus httpStatus = responseEntity.getStatusCode();
		serviceResponse.put("httpStatus", httpStatus);
		
		if (httpStatus.is2xxSuccessful()) {
			Object responseObject = serviceMap.get("response");
			if (responseObject != null) {
				Map<String, Object> responseMap = Collections.unmodifiableMap((Map<String, Object>) responseObject);
				Object headersObject = responseMap.get("headers");
				if (headersObject != null) {
					Map<String, Object> headersMap = Collections.unmodifiableMap((Map<String, Object>) headersObject);
					serviceResponse.putAll(getResponseFromHeaders(headersMap, responseEntity.getHeaders()));
				}
				Object bodyObject = responseMap.get("body");
				if (bodyObject != null) {
					Map<String, Object> bodyMap = Collections.unmodifiableMap((Map<String, Object>) bodyObject);
					serviceResponse.putAll(getResponseFromBody(bodyMap, responseEntity.getBody()));
				}
			}
		} else {
			Object errorObject = serviceMap.get("error");
			if (errorObject != null) {
				Map<String, Object> errorMap = Collections.unmodifiableMap((Map<String, Object>) errorObject);
				Object bodyObject = errorMap.get("body");
				if (bodyObject != null) {
					Map<String, Object> bodyMap = Collections.unmodifiableMap((Map<String, Object>) bodyObject);
					serviceResponse.putAll(getResponseFromBody(bodyMap, responseEntity.getBody()));
				}
			}
		}
		return serviceResponse;
	}

	private Map<String, Object> getResponseFromHeaders(Map<String, Object> headersMap, HttpHeaders httpHeaders) {
		Map<String, Object> returnMap = new HashMap<>();
		String STRING_EMPTY = "";
		headersMap.forEach((x, y) -> {
			String value = httpHeaders.getFirst(y.toString());
			if (value != null && !value.equals(STRING_EMPTY)) {
				returnMap.put(x, value);
			} else {
				if (log.isInfoEnabled()) {
					log.info("Cannot find the key in the headers: {}", x);
				}
			}
		});
		return returnMap;
	}

	private Map<String, Object> getResponseFromBody(Map<String, Object> bodyMap, String content) {
		return getJSONValuesFromPaths(bodyMap, content);

	}

	private String getContentFromFile(String fileName) {
		String content = null;
		try {
			String STRING_CLASSPATH = "classpath:";
			Resource resource = resourceLoader.getResource(STRING_CLASSPATH + fileName);
			InputStream inputStream = resource.getInputStream();
			byte[] bdata = FileCopyUtils.copyToByteArray(inputStream);
			content = new String(bdata, StandardCharsets.UTF_8);
		} catch (IOException e) {
			if (log.isInfoEnabled()) {
				log.info("Reading File Exception: {}", e.toString());
			}
		}
		return content;
	}
	
	
	private String getBody(Map<String, Object> requestMap, Map<String, String> inMap) {
		String fileName = requestMap.get("body").toString();
		String content = getContentFromFile(fileName);
		return fillPlaceHolders(inMap, content);
	}
	
	private String getUri(Map<String, Object> serviceMap, Map<String, String> inMap) {
		String uri = serviceMap.get("uri").toString();
		return fillPlaceHolders(inMap, uri);
	}
	
	@SuppressWarnings("unchecked")
	private HttpHeaders getHttpHeaders(Map<String, Object> requestMap, Map<String, String> inMap) {
		Object headersObject = requestMap.get("headers");
		Map<String, String> headersMap = Collections.unmodifiableMap((Map<String, String>) headersObject);
		HttpHeaders httpHeaders = new HttpHeaders();
		headersMap.forEach((x, y) -> {
			httpHeaders.set(x, fillPlaceHolders(inMap, y));
		});
		return httpHeaders;
	}
	
	
	@SuppressWarnings("unchecked")
	private HttpEntity<String> getHttpEntity(Map<String, Object> serviceMap, Map<String, String> inMap) {

		String method = serviceMap.get("method").toString();
		Object requestObject = serviceMap.get("request");
		Map<String, Object> requestMap = Collections.unmodifiableMap((Map<String, Object>) requestObject);
		HttpHeaders httpHeaders = getHttpHeaders(requestMap, inMap);
		HttpEntity<String> entity;
		
		if (method.equals(HttpMethod.GET.toString())) {
			entity = new HttpEntity<>(httpHeaders);
		} else {
			final String body = getBody(requestMap, inMap);
			entity = new HttpEntity<>(body, httpHeaders);
		}
		if (log.isDebugEnabled()) {
			log.debug("Sending Headers: {}", entity.getHeaders());
			log.debug("Sending Body: {}", entity.getBody());
		}
		return entity;
	}
	
	private ResponseEntity<String> getResponseEntity(Map<String, Object> serviceMap, Map<String, String> inMap) {
		final String uri = getUri(serviceMap, inMap);
		HttpEntity<String> httpEntity = getHttpEntity(serviceMap, inMap);
		
		
		String method = serviceMap.get("method").toString();
		HttpMethod httpMethod = HttpMethod.resolve(method);
		
		ResponseEntity<String> responseEntity = null;
		
		try {
			responseEntity = restTemplate.exchange(uri,  httpMethod, httpEntity, String.class);
		} catch (HttpStatusCodeException e) {
			if (log.isInfoEnabled()) {
				log.info("HttpStatusCodeException: {}", e.toString());
			}
			responseEntity = new ResponseEntity<> (
					e.getResponseBodyAsString(),
					e.getResponseHeaders(),
					e.getStatusCode());
					
		} catch (ResourceAccessException e) {
			String STRING_REASON = "reason";
			if (log.isInfoEnabled()) {
				log.info("ResourceAccessException: {}", e.toString());
			}
			Map<String, String> errorMap = new HashMap<>();
			errorMap.put(STRING_REASON, e.getCause().toString());
			try {
				responseEntity = new ResponseEntity<> (
						mapper.writeValueAsString(errorMap),
						httpEntity.getHeaders(),
						HttpStatus.INTERNAL_SERVER_ERROR);
			} catch (JsonProcessingException ex) {
				throw new RuntimeException(ex);
			}
		}
		return responseEntity;
	}
	
	
	private String fillPlaceHolders(final Map<String, String> substitutesMap, String original) {
		String STRING_UUID = "uuid";
		if (original != null && original.contains(STRING_UUID)) {
			substitutesMap.put(STRING_UUID, UUID.randomUUID().toString());
		}
		return replacePlaceHolders(substitutesMap, original);
	}
	
	private String replacePlaceHolders(Map<String, String> substitutes, String original) {
		String STRING_PLACEHOLDER = "placeholder";
		String STRING_REGEX = "\\$\\{(?<" + STRING_PLACEHOLDER + ">[A-Za-z0-9-_]+)}";
		Pattern tokenPattern = Pattern.compile(STRING_REGEX);
		Function<Matcher, String> converter = (matcher) -> substitutes.get(matcher.group(STRING_PLACEHOLDER));
		
		int lastIndex = 0;
		StringBuilder output = new StringBuilder();
		Matcher matcher = tokenPattern.matcher(original);
		while(matcher.find()) {
			output.append(original, lastIndex, matcher.start()).append(converter.apply(matcher));
			lastIndex = matcher.end();
		}
		if (lastIndex < original.length()) {
			output.append(original, lastIndex, original.length());
		}
		return output.toString();
	}
	
	private Map<String, Object> getJSONValuesFromPaths(Map<String, Object> jsonPathMap, String content) {
		Map<String, Object> returnMap = new HashMap<>();
		String STRING_ROOT_ELEMENT = "$";
		if (content != null && content.length() > 0) {
			jsonPathMap.forEach((x, y) -> {
				try {
					if (y.toString().equals(STRING_ROOT_ELEMENT)) {
						try {
							returnMap.put(x, mapper.readValue(content, new TypeReference<Map<String, Object>>() {

							}));
						} catch (JsonProcessingException e) {
							throw new RuntimeException(e);
						}
					} else {
						Object valueObject = JsonPath.read(content, y.toString());
						if (valueObject != null) {
							returnMap.put(x, valueObject);
						}
					}
				} catch (PathNotFoundException e) {
					if (log.isDebugEnabled()) {
						log.debug("Cannot find the path in the content: {}", y);
					}
				}
			});
		}
		return returnMap;
	}
	
	
	
}
