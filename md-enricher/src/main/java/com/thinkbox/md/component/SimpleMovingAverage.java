package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class SimpleMovingAverage extends Indicator {

	private Queue<Double> queue = new LinkedList<>();

	private double sum = 0;

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
				
		map.put(getKey(), getAverage());

	}
	
	private Double getAverage() {
		if (queue.size() < period) 
			return 0d;
		
		return sum/period;
	}
	
	private String getKey() {
		return "sma-" + period;
	}
}
