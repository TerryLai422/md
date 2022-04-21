package com.thinkbox.md.component;

import java.util.Map;

import com.thinkbox.md.config.MapKeyParameter;

public class PreviousClose extends Indicator {

	private double last = 0d;

	private PreviousClose() {
		super();
	}

	public PreviousClose(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {

		Double close = (Double) map.get(mapKey.getClose());

		map.put(mapKey.getPercentChange(), (close - last)/ last);
		map.put(getKey(), last);
		
		last = close;
	}

	private String getKey() {
		return "previousClose";
	}
}
