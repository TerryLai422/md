package com.thinkbox.md.component;

import java.util.LinkedList;
import java.util.Queue;

public class WeekHighLow {

	private Queue<Integer> dayQueue = new LinkedList<>();

	private Queue<Double> dataQueue = new LinkedList<>();

	private int period;

	private int limit;

	public WeekHighLow(int period) {
		this.period = period;
		if (period == 52) {
			this.limit = 1000;
		} else {
			this.limit = 130;
		}
	}

	public void add(Integer year, Integer day, Double data) {

		Integer formatedDay = year * 1000 + day;
		if (dayQueue.size() > 0) {
			if (formatedDay - dayQueue.peek() >= this.limit) {
				dataQueue.poll();
				dayQueue.poll();
			}
		}
		dataQueue.add(data);
		dayQueue.add(formatedDay);

	}

	public Double getHigh() {
		if (dataQueue.size() > 0) {
			return dataQueue.stream().mapToDouble(x -> x).max().getAsDouble();
		}
		return 0d;
	}

	public Double getLow() {
		if (dataQueue.size() > 0) {
			return dataQueue.stream().mapToDouble(x -> x).min().getAsDouble();
		}
		return 0d;
	}

	public String getPrefix() {
		return "WeekHL" + period + "-";
	}
}
