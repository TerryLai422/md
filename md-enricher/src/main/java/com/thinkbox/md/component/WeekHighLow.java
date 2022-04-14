package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class WeekHighLow {

	private Queue<Integer> dayQueue = new LinkedList<>();

	private Queue<Double> dataQueue = new LinkedList<>();

	private MapKeyParameter mapKey;

	private int period;

	private int limit;

	private Double historicalHigh;
	
	private Double historicalLow;
	
	private String historicalHighDate;
	
	private String historicalLowDate;
	
	public WeekHighLow(MapKeyParameter mapKey, int period) {
		this.mapKey = mapKey;
		this.period = period;
		
		if (period == 52) {
			this.limit = 1000;
		} else {
			this.limit = 130;
		}
	}

	public void add(Map<String, Object> map) {

		Integer year = (Integer) map.get(mapKey.getYear());
		Integer dayOfYear = (Integer) map.get(mapKey.getDayOfYear());
		String date = map.get(mapKey.getDate()).toString();
		Double close = (Double) map.get(mapKey.getClose());
		
		Integer formatedDay = year * 1000 + dayOfYear;
		if (dayQueue.size() > 0) {
			if (formatedDay - dayQueue.peek() >= this.limit) {
				dataQueue.poll();
				dayQueue.poll();
			}
		}
		if (historicalHigh == null || close > historicalHigh) {
			historicalHigh = close;
			historicalHighDate = date;
		}
		if (historicalLow == null || close < historicalLow) {
			historicalLow = close;
			historicalLowDate = date;			
		}
		dataQueue.add(close);
		dayQueue.add(formatedDay);
		
		Double high = getHigh();
		Double low = getLow();
		
		map.put(mapKey.getNewHigh() + period + "W", close.equals(high));
		map.put(mapKey.getNewLow() + period + "W", close.equals(low));
		map.put(mapKey.getHistoricalHigh(), historicalHigh);
		map.put(mapKey.getHistoricalHighDate(), historicalHighDate);
		map.put(mapKey.getHistoricalLow(), historicalLow);
		map.put(mapKey.getHistoricalLowDate(), historicalLowDate);
		map.put(getPrefix() + mapKey.getSuffixHigh(), high);
		map.put(getPrefix() + mapKey.getSuffixLow(), low);

	}

	private Double getHigh() {
		if (dataQueue.size() > 0) {
			return dataQueue.stream().mapToDouble(x -> x).max().getAsDouble();
		}
		return 0d;
	}

	private Double getLow() {
		if (dataQueue.size() > 0) {
			return dataQueue.stream().mapToDouble(x -> x).min().getAsDouble();
		}
		return 0d;
	}

	private String getPrefix() {
		return "weekHL-" + period + "W-";
	}
}
