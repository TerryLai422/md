package com.thinkbox.md.component;

import java.util.Map;

import com.thinkbox.md.config.MapKeyParameter;

public abstract class Indicator {
	
	protected MapKeyParameter mapKey;

	protected int period;

	protected Indicator() {
		
	}
	
	public Indicator(MapKeyParameter mapKey, int period) {
		this.mapKey = mapKey;
		this.period = period;
	}

	abstract public void process(Map<String, Object> map);
}
