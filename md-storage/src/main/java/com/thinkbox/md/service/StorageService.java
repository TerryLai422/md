package com.thinkbox.md.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.repository.HistoricalRepository;

@Component
public class StorageService {

	@Autowired
	private HistoricalRepository historicalRepository;

	public void saveHistoricalList(List<Map<String, Object>> list) {
		
		List<Historical> historicalList = list.stream().map(DataMapper::convertHistorical).collect(Collectors.toList());
		
		historicalRepository.saveAll(historicalList);
	
	}
	
	public void saveHistorical(Map<String, Object> map) {
	
		Historical historical = DataMapper.convertHistorical(map);
		
		historicalRepository.save(historical);
		
	}
	
}
