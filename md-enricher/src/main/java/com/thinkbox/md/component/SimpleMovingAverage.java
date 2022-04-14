package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class SimpleMovingAverage {

	private Queue<Double> queue = new LinkedList<>();

	private MapKeyParameter mapKey;

	private int period;
	
	private int size = 0;
	
	private double sum = 0;
	
	private double first = 0;
	
	public SimpleMovingAverage(MapKeyParameter mapKey, int period) {
		this.mapKey = mapKey;
		this.period = period;
	}

	public void add(Map<String, Object> map) {
		Double close = (Double) map.get(mapKey.getClose());
		
		if (size >= period) {
			first = queue.poll();
			sum -= first;
		} else {
			size++;
		}
 		
		queue.add(close);
		sum += close;
		
		map.put(getPrefix() + mapKey.getSuffixValue(), getAverage());
		map.put(getPrefix() + mapKey.getSuffixSum(), sum);
		map.put(getPrefix() + mapKey.getSuffixFirst(), first);
		map.put(getPrefix() + mapKey.getSuffixSize(), size);
	}
	
	private Double getAverage() {
		if (size < period) 
			return 0d;
		
		return sum/period;
	}
	
	private String getPrefix() {
		return "sma-" + period + "-";
	}
}
