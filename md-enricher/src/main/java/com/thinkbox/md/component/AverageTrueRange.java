package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class AverageTrueRange extends Indicator {

	private Queue<Double> queue = new LinkedList<>();

	private double sum = 0d;

	private AverageTrueRange() {
		super();
	}

	public AverageTrueRange(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {

		Double prevClose = (Double) map.getOrDefault(mapKey.getPrevClose(), 0d);
		Double high = (Double) map.get(mapKey.getHigh());
		Double low = (Double) map.get(mapKey.getLow());
		Double max = 0d;
		if (prevClose == 0) {
			max = high - low;
		} else {
			max = Math.max(high - low, Math.max(prevClose - low, Math.abs(prevClose - high)));
		}
		
		queue.add(max);
		sum += max;

		
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
		return "atr" + period;
	}
}
