package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Queue;

import lombok.Getter;

public class SimpleMovingAverage {

	private Queue<Double> queue = new LinkedList<>();
	
	private int period;
	
	@Getter
	private int size = 0;
	
	@Getter
	private double sum = 0;
	
	@Getter
	private double first = 0;
	
	public SimpleMovingAverage(int period) {
		this.period = period;
	}
	
	public void add(Double data) {
		
		if (size >= period) {
			first = queue.poll();
			sum -= first;
		} else {
			size++;
		}
 		
		queue.add(data);
		sum += data;
	}
	
	public Double getAverage() {
		if (size < period) 
			return 0d;
		
		return sum/period;
	}
	
	public String getPrefix() {
		return "SMA-" + period + "-";
	}
}
