package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class OnBalanceVolume extends Indicator {

	private Queue<Long> queue = new LinkedList<>();

	private Double last = 0d;

	private Long sum = 0L;

	private OnBalanceVolume() {
		super();
	}

	public OnBalanceVolume(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void add(Map<String, Object> map) {

		Double close = (Double) map.get(mapKey.getClose());

		long volume = 0L;

		Object vol = map.get(mapKey.getVolume());
		if (vol.getClass().equals(Integer.class)) {
			volume = (Integer) vol;
		} else {
			volume = (Long) vol;
		}

		if (last != 0) {
			if (close > last) {
				sum += volume;
			} else {
				sum -= volume;
			}
		}

		last = close;

		map.put(getPrefix(), sum);
	}

	private String getPrefix() {
		return "OBV";
	}
}
