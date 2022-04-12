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

import com.thinkbox.md.config.ParameterProperties;

@Component
public class EnricherService {

	private final Logger logger = LoggerFactory.getLogger(EnricherService.class);
	
	@Autowired
	private ParameterProperties parameter;
	
	public List<Map<String, Object>> consolidateWeekly(List<Map<String, Object>> list) {
		
		return consolidate(parameter.getValueWeekly(), parameter.getKeyYearForWeek(), parameter.getKeyWeekOfYear(), list);
	}

	public List<Map<String, Object>> consolidateMonthly(List<Map<String, Object>> list) {
		
		return consolidate(parameter.getValueMonthly(), parameter.getKeyYear(), parameter.getKeyMonth(), list);
	}

	public List<Map<String, Object>> consolidate(final String type, final String firstCriterion, final String secondCriterion, List<Map<String, Object>> list) {
		
		Map<Object, List<Map<String, Object>>> inputMapList = list.stream().skip(1)
				.collect(Collectors.groupingBy(
						x -> new ArrayList<Integer>(
								Arrays.asList((Integer) x.get(firstCriterion), (Integer) x.get(secondCriterion))),
						Collectors.toList()));

		List<Map<String, Object>> outputMapList = inputMapList.values().stream().map(x -> {

			List<Map<String, Object>> sortedList = x.stream()
					.sorted((i, j) -> i.get(parameter.getKeyDate()).toString().compareTo(j.get(parameter.getKeyDate()).toString()))
					.collect(Collectors.toList());

			Map<String, Object> first = sortedList.get(0);
			Map<String, Object> last = sortedList.get(sortedList.size() - 1);

			Map<String, Object> map = new TreeMap<String, Object>();

			map.put(parameter.getKeyType(), type);
			map.put(parameter.getKeySymbol(), last.get(parameter.getKeySymbol()));
			map.put(parameter.getKeyDate(), last.get(parameter.getKeyDate()));
			map.put(parameter.getKeyOpen(), first.get(parameter.getKeyOpen()));
			map.put(parameter.getKeyClose(), last.get(parameter.getKeyClose()));
			map.put(parameter.getKeyAdjClose(), last.get(parameter.getKeyAdjClose()));
			map.put(parameter.getKeyYear(), last.get(parameter.getKeyYear()));
			map.put(parameter.getKeyMonth(), last.get(parameter.getKeyMonth()));
			map.put(parameter.getKeyDay(), last.get(parameter.getKeyDay()));
			map.put(parameter.getKeyWeekOfYear(), last.get(parameter.getKeyWeekOfYear()));

			map.put(parameter.getKeyHigh(), x.stream().mapToDouble(a -> (Double) a.get(parameter.getKeyHigh())).max().getAsDouble());
			map.put(parameter.getKeyLow(), x.stream().mapToDouble(a -> (Double) a.get(parameter.getKeyLow())).min().getAsDouble());
			map.put(parameter.getKeyVolume(), x.stream().mapToLong(a -> Long.valueOf(a.get(parameter.getKeyVolume()).toString())).sum());

			return map;

		}).collect(Collectors.toList());

		return outputMapList;
	}
}