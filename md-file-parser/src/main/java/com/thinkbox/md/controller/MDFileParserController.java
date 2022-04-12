package com.thinkbox.md.controller;

import java.io.IOException;
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

import com.thinkbox.md.service.FileParserService;

@ConditionalOnProperty(name = "md-file-parser.controller.enabled", havingValue = "true")
@RestController
@RequestMapping("")
public class MDFileParserController {

	private final Logger logger = LoggerFactory.getLogger(MDFileParserController.class);

	@Autowired
	FileParserService fileParserService;

	@PostMapping(path = "parseExchange", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Object> parse(@RequestBody Map<String, Object> map) {
		logger.info("Received Map: {}", map.toString());
		String exchange = (String) map.get("exchange");

		try {
			return ResponseEntity.ok(fileParserService.parseExchangeFile(exchange));
		} catch (IOException e) {
			return ResponseEntity.internalServerError().body(e.toString());
		}
	}

}
