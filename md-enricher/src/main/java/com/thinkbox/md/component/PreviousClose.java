package com.thinkbox.md.component;

import java.util.Map;

import com.thinkbox.md.config.MapKeyParameter;

public class PreviousClose extends Indicator {

	final private String PREFIX_STRING = "prevClose";

	private double last = 0d;

	private PreviousClose() {
		super();
	}

	public PreviousClose(MapKeyParameter mapKey, int period) {
		super(mapKey, period);
	}

	public void process(Map<String, Object> map) {

		Double close = (Double) map.get(mapKey.getClose());

		@SuppressWarnings("unchecked")
		Map<String, Object> ind = (Map<String, Object>) map.get(mapKey.getInd());

		ind.put(mapKey.getPercentChange(), (close - last)/ last);
		ind.put(getKey(), last);
		
		last = close;
	}

	private String getKey() {
		return "prevClose";
	}
}
