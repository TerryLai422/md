package com.thinkbox.md.controller;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.md.service.FileParseService;

import lombok.extern.slf4j.Slf4j;

@ConditionalOnProperty(name = "md-file-parser.controller.enabled", havingValue = "true")
@RestController
@RequestMapping("")
@Slf4j
public class MDFileParserController {

	@Autowired
	FileParseService fileParserService;

	@PostMapping(path = "parseExchange", consumes = "application/json", produces = "application/json")
	public ResponseEntity<Object> parse(@RequestBody Map<String, Object> map) {
		log.info("Received Map: {}", map.toString());
		String exchange = (String) map.get("exchange");

		try {
			return ResponseEntity.ok(fileParserService.parseExchangeFile(exchange));
		} catch (IOException e) {
			return ResponseEntity.internalServerError().body(e.toString());
		}
	}

}
