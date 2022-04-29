package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class WeekHighLow extends Indicator {

	final private String PREFIX_STRING = "week";
	
	final private String SUFFIX_STRING = "W";
	
	private Queue<Integer> dayQueue = new LinkedList<>();

	private Queue<Double> dataQueue = new LinkedList<>();

	private int limit;

	private double historicalHigh = 0;
	
	private double historicalLow = 0;
	
	private String historicalHighDate;
	
	private String historicalLowDate;
	
	private WeekHighLow() {
		super();
	}
	
	public WeekHighLow(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
		
		if (period == 52) {
			this.limit = 1000;
		} else {
			this.limit = 130;
		}
	}

	public void process(Map<String, Object> map) {

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
		if (historicalHigh == 0 || close > historicalHigh) {
			historicalHigh = close;
			historicalHighDate = date;
		}
		if (historicalLow == 0 || close < historicalLow) {
			historicalLow = close;
			historicalLowDate = date;			
		}
		dataQueue.add(close);
		dayQueue.add(formatedDay);
		
		Double high = getHigh();
		Double low = getLow();
		
		@SuppressWarnings("unchecked")
		Map<String, Object> ind = (Map<String, Object>) map.get(mapKey.getInd());

		ind.put(mapKey.getNewHigh() + period + SUFFIX_STRING, close.equals(high));
		ind.put(mapKey.getNewLow() + period + SUFFIX_STRING, close.equals(low));
		ind.put(mapKey.getHistoricalHigh(), historicalHigh);
		ind.put(mapKey.getHistoricalHighDate(), historicalHighDate);
		ind.put(mapKey.getHistoricalLow(), historicalLow);
		ind.put(mapKey.getHistoricalLowDate(), historicalLowDate);
		ind.put(getPrefix() + mapKey.getSuffixHigh(), high);
		ind.put(getPrefix() + mapKey.getSuffixLow(), low);

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
		return PREFIX_STRING + period;
	}
}
