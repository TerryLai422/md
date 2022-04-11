package com.thinkbox.md.mapper;

import java.util.Map;

import com.thinkbox.md.model.Historical;

public class DataMapper {

	public static Historical convertHistorical(Map<String, Object> map) {
		Historical historical = new Historical();
		
		historical.setId(map.get("type") + "-" + map.get("symbol") + "@" + map.get("date"));
		historical.setType((String) map.get("type"));
		historical.setSymbol((String) map.get("symbol"));
		historical.setYear((Integer) map.get("year"));
		historical.setMonth((Integer) map.get("month"));		
		historical.setDay((Integer) map.get("day"));	
		historical.setWeekOfYear((Integer) map.get("weekOfYear"));	
		historical.setDayOfWeek((Integer) map.get("dayOfWeek"));	
		historical.setOpen((Double) map.get("open"));	
		historical.setHigh((Double) map.get("high"));	
		historical.setLow((Double) map.get("low"));	
		historical.setClose((Double) map.get("close"));	
		historical.setAdjClose((Double) map.get("adjClose"));	
		historical.setVolume((Long) map.get("volume"));	
		historical.setDetails(map);	
		
		return historical; 
	}
}