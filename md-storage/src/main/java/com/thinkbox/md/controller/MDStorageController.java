package com.thinkbox.md.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.md.repository.HistoricalRepository;

@ConditionalOnProperty(name = "md-storage.controller.enabled", havingValue = "true")
//@ConditionalOnExpression("${my.controller.enabled:true}")
@RestController
@RequestMapping("")
public class MDStorageController {

	private final Logger logger = LoggerFactory.getLogger(MDStorageController.class);

	@Autowired
	private HistoricalRepository historicalRepository;

	@PostMapping(path = "test1", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Object> test(@RequestBody Map<String, Object> map) {
		logger.info("Received Map: {}", map.toString());
//		Historical historical = HistoricalMapper.convert(map);

//		Historical saved = historicalRepository.insert(historical);
		return ResponseEntity.ok(map);
	}

	
	@PostMapping(path = "test2", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Object> saveExchange(@RequestBody Map<String, Object> map) {
		logger.info("Received Map: {}", map.toString());

		return ResponseEntity.ok(map);
	}

}
