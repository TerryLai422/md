package com.thinkbox.md.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.repository.HistoricalRepository;
import com.thinkbox.md.repository.InstrumentRepository;

@Component
public class StoreService {

	@Autowired
	private HistoricalRepository historicalRepository;

	@Autowired
	private InstrumentRepository instrumentRepository;
	
	@Autowired
	private DataMapper mapper;
	
	public void saveHistoricalList(List<Map<String, Object>> list) {
		
		List<Historical> convertedList = list.stream().skip(1).map(mapper::convertMapToHistorical).collect(Collectors.toList());
		
		historicalRepository.saveAll(convertedList);
	
	}
	
	public void saveHistorical(Map<String, Object> map) {
	
		Historical historical = mapper.convertMapToHistorical(map);
		
		historicalRepository.save(historical);
		
	}
	
	public void saveInstrumentList(List<Map<String, Object>> list) {
		
		List<Instrument> convertedList = list.stream().skip(1).map(mapper::convertMapToInstrument).collect(Collectors.toList());
		
		instrumentRepository.saveAll(convertedList);
	
	}
	
	public void saveInstrument(Map<String, Object> map) {
	
		Instrument instrument = mapper.convertMapToInstrument(map);
		
		instrumentRepository.save(instrument);
		
	}

	public List<Map<String, Object>> getInstruments(final String subExchange) {
			
		List<Instrument> instruments = instrumentRepository.findBySubExchange(subExchange);
		
		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());
		
	}

}
