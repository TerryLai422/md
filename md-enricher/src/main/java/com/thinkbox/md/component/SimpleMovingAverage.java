package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class SimpleMovingAverage extends Indicator {

	final private String PREFIX_STRING = "sma";

	private Queue<Double> queue = new LinkedList<>();

	private double sum = 0;

	private SimpleMovingAverage() {
		super();
	}
	
	public SimpleMovingAverage(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {
		Double close = (Double) map.get(mapKey.getClose());
		
		queue.add(close);
		sum += close;

		
		if (queue.size() > period) {
			sum -= queue.poll();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> ind = (Map<String, Object>) map.get(mapKey.getInd());
		
		ind.put(getKey(), getAverage());
		ind.put(getKey() + "-sum", sum);
		ind.put(getKey() + "-count", queue.size());
		ind.put(getKey() + "-first", queue.peek());

	}
	
	private Double getAverage() {
		if (queue.size() < period) 
			return 0d;
		
		return sum/period;
	}
	
	private String getKey() {
		return PREFIX_STRING + period;
	}
}
