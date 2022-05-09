package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.thinkbox.md.config.MapKeyParameter;

public class OnBalanceVolume extends Indicator {

	final private String PREFIX_STRING = "obv";

	private Queue<Long> queue = new LinkedList<>();

	private double last = 0d;

	private long sum = 0L;

	private OnBalanceVolume() {
		super();
	}

	public OnBalanceVolume(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {

		Double close = (Double) map.get(mapKey.getClose());

		long volume = 0L;

		@SuppressWarnings("unchecked")
		Map<String, Object> indMap = (Map<String, Object>) map.get(mapKey.getInd());

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
		} else {
			if (sum == 0) {
				if (indMap != null) {
					if (indMap.containsKey(getKey())) {
						System.out.println("Key: " + getKey());
						System.out.println("existing OBV1: " + indMap.get(getKey()));

						Object existingOBV = indMap.get(getKey());
						if (existingOBV.getClass().equals(Integer.class)) {
							sum = (Integer) existingOBV;
						} else {
							sum = (Long) existingOBV;
						}
					}
				}
			}
		}

		last = close;

		indMap.put(getKey(), sum);
	}

	private String getKey() {
		return PREFIX_STRING;
	}
}
