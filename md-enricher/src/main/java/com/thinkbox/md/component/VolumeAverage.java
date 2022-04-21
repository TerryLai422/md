package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class VolumeAverage extends Indicator {

	private Queue<Long> queue = new LinkedList<>();

	private long sum = 0L;

	private VolumeAverage() {
		super();
	}
	
	public VolumeAverage(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {

		long volume = 0L;
		
		Object vol = map.get(mapKey.getVolume());
		if (vol.getClass().equals(Integer.class)) {
			volume = (Integer) vol;
		} else {
			volume = (Long) vol;
		}

		queue.add(volume);
		sum += volume;

		
		if (queue.size() > period) {
			sum -= queue.poll();
		}
				
		map.put(getKey(), getAverage());

	}

	private double getAverage() {
		if (queue.size() < period)
			return 0d;

		return sum / period;
	}

	private String getKey() {
		return "AvgVol" + period;
	}
}
