package com.thinkbox.md.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
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

	public Map<String, Object> getInstrument(final String ticker) {
		
		List<Instrument> instruments = instrumentRepository.findByTicker(ticker);
		if (instruments != null && instruments.size() > 0) {
			return instruments.get(0).getOthers();
		}
		return null;
	}
	
	public List<Map<String, Object>> getInstruments(final String subExchange) {
		
		List<Instrument> instruments = instrumentRepository.findBySubExchange(subExchange);
		
		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());
		
	}
	
	public List<Map<String, Object>> getHistoricalTotalFromInstrument(final String subExchange, int limit) {
			
		List<Instrument> instruments = instrumentRepository.findBySubExchange(subExchange);

		return instruments.stream().filter(x -> x.getHistoricalTotal() <= limit).map(x -> x.getOthers()).collect(Collectors.toList());
		
	}
	
	public List<Map<String, Object>> getHistoricals(int page) {
		
		PageRequest pageRequest = PageRequest.of(page, 2000);
		
		List<Historical> historicals = historicalRepository.findAllNull(pageRequest);
		
		return historicals.stream().limit(2000).map(x -> x.getOthers()).collect(Collectors.toList());
		
	}

	public List<Map<String, Object>> getHistoricals(final String ticker) {
		
		List<Historical> historicals = historicalRepository.findByTicker(ticker);
		
		return historicals.stream().map(x -> x.getOthers()).collect(Collectors.toList());
		
	}
	
	public List<Map<String, Object>> getHistoricals(final String ticker, long limit) {
		
		List<Historical> historicals = Arrays.asList();
		
		if (historicalRepository.countByTicker(ticker) <= limit) {
			historicals = historicalRepository.findByTicker(ticker);
		}
		
		return historicals.stream().map(x -> x.getOthers()).collect(Collectors.toList());
	}

	public Map<String, Object> getHistoricalSummary(final String ticker) {
		
		HistoricalSummary summary = null;
		if (historicalRepository.countByTicker(ticker) > 0) {
			List<HistoricalSummary> list = historicalRepository.getSummary(ticker);
			summary = list.get(0);
		}
		
		return mapper.convertHistoricalSummaryToMap(summary);
	}
	
	public Long getHistoricalsTotal(final String ticker) {

		return historicalRepository.countByTicker(ticker);
		
	}
}
