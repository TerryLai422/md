package com.thinkbox.md.mapper;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.Instrument;

@Component
public class DataMapper {

	@Autowired
	private MapKeyParameter mapKey;

	public Historical convertHistorical(Map<String, Object> map) {
		
		Historical historical = new Historical();
		
		historical.setId(map.get(mapKey.getType()) + "-" + map.get(mapKey.getSymbol()) + "@" + map.get(mapKey.getDate()));
		historical.setType((String) map.get(mapKey.getType()));
		historical.setSymbol((String) map.get(mapKey.getSymbol()));
		historical.setYear((Integer) map.get(mapKey.getYear()));
		historical.setMonth((Integer) map.get(mapKey.getMonth()));		
		historical.setDay((Integer) map.get(mapKey.getDay()));	
		historical.setWeekOfYear((Integer) map.get(mapKey.getWeekOfYear()));	
		historical.setDayOfYear((Integer) map.get(mapKey.getDayOfYear()));	
		historical.setDayOfWeek((Integer) map.get(mapKey.getDayOfWeek()));	
		historical.setOpen((Double) map.get(mapKey.getOpen()));	
		historical.setHigh((Double) map.get(mapKey.getHigh()));	
		historical.setLow((Double) map.get(mapKey.getLow()));	
		historical.setClose((Double) map.get(mapKey.getClose()));	
		historical.setAdjClose((Double) map.get(mapKey.getAdjClose()));	
		historical.setVolume((Long) map.get(mapKey.getVolume()));	
		historical.setDetails(map);	
		
		return historical; 
	}
	
	public Instrument convertInstrument(Map<String, Object> map) {
		
		Instrument instrument = new Instrument();
		
		instrument.setId(map.get(mapKey.getExchange()) + "@" + map.get(mapKey.getSymbol()));
		instrument.setExchange((String) map.get(mapKey.getExchange()));
		instrument.setSymbol((String) map.get(mapKey.getSymbol()));
		instrument.setName((String) map.get(mapKey.getName()));
		
		return instrument; 
	}
}