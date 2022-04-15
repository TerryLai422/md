package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class SimpleMovingAverage extends Indicator {

	private Queue<Double> queue = new LinkedList<>();

	private double sum = 0;
	
	private double first = 0;	

	private SimpleMovingAverage() {
		super();
	}
	
	public SimpleMovingAverage(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void add(Map<String, Object> map) {
		Double close = (Double) map.get(mapKey.getClose());
		
		queue.add(close);
		sum += close;

		
		if (queue.size() > period) {
			sum -= queue.poll();
		}
				
		first = queue.peek();

		
		map.put(getPrefix() + mapKey.getSuffixValue(), getAverage());
		map.put(getPrefix() + mapKey.getSuffixSum(), sum);
		map.put(getPrefix() + mapKey.getSuffixFirst(), first);
		map.put(getPrefix() + mapKey.getSuffixSize(), queue.size());
	}
	
	private Double getAverage() {
		if (queue.size() < period) 
			return 0d;
		
		return sum/period;
	}
	
	private String getPrefix() {
		return "sma-" + period + "-";
	}
}
