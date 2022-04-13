package com.thinkbox.md.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;

@Component
public class EnricherService {

	private final Logger logger = LoggerFactory.getLogger(EnricherService.class);
	
	@Autowired
	private MapKeyParameter mapKey;
	
	@Autowired
	private MapValueParameter mapValue;

	public List<Map<String, Object>> consolidateWeekly(List<Map<String, Object>> list) {
		
		return consolidate(mapValue.getWeekly(), mapKey.getYearForWeek(), mapKey.getWeekOfYear(), list);
	}

	public List<Map<String, Object>> consolidateMonthly(List<Map<String, Object>> list) {
		
		return consolidate(mapValue.getMonthly(), mapKey.getYear(), mapKey.getMonth(), list);
	}

	public List<Map<String, Object>> consolidate(final String type, final String firstCriterion, final String secondCriterion, List<Map<String, Object>> list) {
		
		Map<Object, List<Map<String, Object>>> inputMapList = list.stream().skip(1)
				.collect(Collectors.groupingBy(
						x -> new ArrayList<Integer>(
								Arrays.asList((Integer) x.get(firstCriterion), (Integer) x.get(secondCriterion))),
						Collectors.toList()));

		List<Map<String, Object>> outputMapList = inputMapList.values().stream().map(x -> {

			List<Map<String, Object>> sortedList = x.stream()
					.sorted((i, j) -> i.get(mapKey.getDate()).toString().compareTo(j.get(mapKey.getDate()).toString()))
					.collect(Collectors.toList());

			Map<String, Object> first = sortedList.get(0);
			Map<String, Object> last = sortedList.get(sortedList.size() - 1);

			Map<String, Object> map = new TreeMap<String, Object>();

			map.put(mapKey.getType(), type);
			map.put(mapKey.getSymbol(), last.get(mapKey.getSymbol()));
			map.put(mapKey.getDate(), last.get(mapKey.getDate()));
			map.put(mapKey.getOpen(), first.get(mapKey.getOpen()));
			map.put(mapKey.getClose(), last.get(mapKey.getClose()));
			map.put(mapKey.getAdjClose(), last.get(mapKey.getAdjClose()));
			map.put(mapKey.getYear(), last.get(mapKey.getYear()));
			map.put(mapKey.getMonth(), last.get(mapKey.getMonth()));
			map.put(mapKey.getDay(), last.get(mapKey.getDay()));
			map.put(mapKey.getWeekOfYear(), last.get(mapKey.getWeekOfYear()));

			map.put(mapKey.getHigh(), x.stream().mapToDouble(a -> (Double) a.get(mapKey.getHigh())).max().getAsDouble());
			map.put(mapKey.getLow(), x.stream().mapToDouble(a -> (Double) a.get(mapKey.getLow())).min().getAsDouble());
			map.put(mapKey.getVolume(), x.stream().mapToLong(a -> Long.valueOf(a.get(mapKey.getVolume()).toString())).sum());

			return map;

		}).collect(Collectors.toList());

		return outputMapList;
	}
}