package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class VolumeAverage extends Indicator {

	private Queue<Long> queue = new LinkedList<>();

	private int size = 0;

	private Long sum = 0L;

	private Long first;

	private VolumeAverage() {
		super();
	}
	
	public VolumeAverage(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void add(Map<String, Object> map) {

		long volume = 0L;
		
		Object vol = map.get(mapKey.getVolume());
		if (vol.getClass().equals(Integer.class)) {
			volume = (Integer) vol;
		} else {
			volume = (Long) vol;
		}

		if (size >= period) {
			first = queue.poll();
			sum -= first;
		} else {
			size++;
		}

		queue.add(volume);
		sum += volume;

		map.put(getPrefix() + mapKey.getSuffixValue(), getAverage());
		map.put(getPrefix() + mapKey.getSuffixSum(), sum);
		map.put(getPrefix() + mapKey.getSuffixFirst(), first);
		map.put(getPrefix() + mapKey.getSuffixSize(), size);

	}

	private Double getAverage() {
		if (size < period)
			return 0d;

		return sum.doubleValue() / period;
	}

	private String getPrefix() {
		return "AvgVol" + period + "-";
	}
}
