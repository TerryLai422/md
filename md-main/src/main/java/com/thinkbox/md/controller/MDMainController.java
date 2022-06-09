package com.thinkbox.md.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.md.config.RestParameterProperties;
import com.thinkbox.md.service.KafkaService;
import com.thinkbox.md.service.MainService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("")
@Slf4j
public class MDMainController {

	@Autowired
	private RestParameterProperties restParameterProperties;

	@Autowired
	private KafkaService kafKaService;

	@Autowired
	private MainService mainService;

	private static final String APPLICATION_JSON_TYPE = "application/json";

	@PostMapping(consumes = APPLICATION_JSON_TYPE, produces = APPLICATION_JSON_TYPE)
	public ResponseEntity<Object> process(@RequestBody Map<String, Object> map) {
		log.info("Received map: {}", map.toString());

		Object objNext = map.get("next");
		int next = 0;
		if (objNext == null) {
			map.put("next", next);
		} else {
			next = Integer.valueOf(objNext.toString());
		}

		Object objStep = map.get("steps");
		if (objStep == null) {
			log.info("Missing Steps -> {}", map.toString());
			return ResponseEntity.badRequest().body("Missing Steps");
		}

		@SuppressWarnings("unchecked")
		List<String> stepList = (List<String>) objStep;
		if (stepList.size() == 0) {
			log.info("Empty Step -> {}", map.toString());
			return ResponseEntity.badRequest().body("Empty Step");
		}

		String topic = stepList.get(next).toString();

		if (topic != null) {
			if (topic.equals("delete-files")) {
				mainService.cleanupFolders();
			} else {
				List<String> parameterList = restParameterProperties.getTopic().get(topic);

				if (parameterList == null) {
					log.info("Cannot find matched topic configuration -> {}", topic);
					return ResponseEntity.badRequest().body("Cannot find matched topic configuration: " + topic);
				} else {
					String missing = "";

					for (String parameter : parameterList) {
						if (!map.containsKey(parameter)) {
							missing += parameter + ";";
						}
					}

					if (!missing.equals("")) {
						log.info("Missing Parameter -> {}", missing);
						return ResponseEntity.badRequest().body("Missing Parameter: " + missing);

					}

					kafKaService.publish(topic, map);
				}
			}
		} else {
			log.info("Incorrect Step -> {}", map.toString());
			return ResponseEntity.badRequest().body("Incorrect Step");
		}

		return ResponseEntity.ok(map);
	}

}