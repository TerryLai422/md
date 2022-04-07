package com.thinkbox.md.controller;

import java.util.Date;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.md.service.FileParserService;

@RestController
@RequestMapping("")
public class MDFileParserController {
	
	@Autowired
	FileParserService fileParserService;

	@PostMapping(path = "parseExchange", consumes = "application/json", produces = "application/json")
    public ResponseEntity<Object> parse(@RequestBody Map<String, Object> map) {
        System.out.println("Map: " + map.toString());
        String exchange = (String) map.get("exchange");
        
        return ResponseEntity.ok(fileParserService.parseExchangeFile(exchange));    
    }
	
}
