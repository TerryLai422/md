package com.thinkbox.md.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ConsolidatorService {

	private final Logger logger = LoggerFactory.getLogger(ConsolidatorService.class);

	public List<Map<String, Object>> consolidateWeekly(List<Map<String, Object>> list) {
		
		return consolidate("weekly", "yearForWeek", "weekOfYear", list);
	}

	public List<Map<String, Object>> consolidateMonthly(List<Map<String, Object>> list) {
		
		return consolidate("monthly", "year", "month", list);
	}

	public List<Map<String, Object>> consolidate(final String type, final String firstCriterion, final String secondCriterion, List<Map<String, Object>> list) {
		
		Map<Object, List<Map<String, Object>>> inputMapList = list.stream().skip(1)
				.collect(Collectors.groupingBy(
						x -> new ArrayList<Integer>(
								Arrays.asList((Integer) x.get(firstCriterion), (Integer) x.get(secondCriterion))),
						Collectors.toList()));

		List<Map<String, Object>> outputMapList = inputMapList.values().stream().map(x -> {

			List<Map<String, Object>> sortedList = x.stream()
					.sorted((i, j) -> i.get("date").toString().compareTo(j.get("date").toString()))
					.collect(Collectors.toList());

			Map<String, Object> first = sortedList.get(0);
			Map<String, Object> last = sortedList.get(sortedList.size() - 1);

			Map<String, Object> map = new TreeMap<String, Object>();

			map.put("type", type);
			map.put("symbol", last.get("symbol"));
			map.put("date", last.get("date"));
			map.put("open", first.get("open"));
			map.put("close", last.get("close"));
			map.put("adjClose", last.get("adjClose"));
			map.put("year", last.get("year"));
			map.put("month", last.get("month"));
			map.put("day", last.get("day"));
			map.put("weekOfYear", last.get("weekOfYear"));

			map.put("high", x.stream().mapToDouble(a -> (Double) a.get("high")).max().getAsDouble());
			map.put("low", x.stream().mapToDouble(a -> (Double) a.get("low")).min().getAsDouble());
			map.put("volume", x.stream().mapToLong(a -> Long.valueOf(a.get("volume").toString())).sum());

			return map;

		}).collect(Collectors.toList());

		return outputMapList;
	}
}