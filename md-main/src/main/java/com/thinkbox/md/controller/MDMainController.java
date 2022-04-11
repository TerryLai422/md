package com.thinkbox.md.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.md.config.RestParameterProperties;
import com.thinkbox.md.service.KafkaService;

@RestController
@RequestMapping("")
public class MDMainController {

	private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	private RestParameterProperties restParameterProperties;

	@Autowired
	private KafkaService kafKaService;

	private static final String APPLICATION_JSON_TYPE = "application/json";

	@PostMapping(consumes = APPLICATION_JSON_TYPE, produces = APPLICATION_JSON_TYPE)
	public ResponseEntity<Object> process(@RequestBody Map<String, Object> map) {
		logger.info("Received map: {}", map.toString());
		String topic = map.getOrDefault("topic", "-").toString();

		if (!topic.equals("-")) {
			List<String> parameterList = restParameterProperties.getTopic().get(topic);

			if (parameterList == null) {
				logger.info("Cannot find topic: {}", topic);
				return ResponseEntity.badRequest().body("Cannot find topic: " + topic);
			} else {
				String missing = "";

				for (String parameter : parameterList) {
					if (!map.containsKey(parameter)) {
						missing += parameter + ";";
					}
				}

				if (!missing.equals("")) {
					logger.info("Missing parameter -> {}", missing);
					return ResponseEntity.badRequest().body("Missing Parameter: " + missing);

				}
				kafKaService.publish(topic, map);

			}
		} else {
			logger.info("Missing topic -> {}", map.toString());
			return ResponseEntity.badRequest().body("Missing Topic");
		}

		return ResponseEntity.ok(map);
	}

}