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

import com.thinkbox.md.component.SimpleMovingAverage;
import com.thinkbox.md.component.WeekHighLow;
import com.thinkbox.md.config.IndicatorProperties;
import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;

@Component
public class EnrichService {

	private final Logger logger = LoggerFactory.getLogger(EnrichService.class);

	@Autowired
	private MapKeyParameter mapKey;

	@Autowired
	private MapValueParameter mapValue;

	@Autowired
	private IndicatorProperties indicatorProperties;

	private List<SimpleMovingAverage> getSMAList() {
		List<SimpleMovingAverage> list = new ArrayList<>();
		for (Integer i : indicatorProperties.getSma()) {
			list.add(new SimpleMovingAverage(i));
		}
		return list;
	}

	public List<Map<String, Object>> enrich(List<Map<String, Object>> list) {
		final List<SimpleMovingAverage> smaList = getSMAList();
		final WeekHighLow weekHighLow = new WeekHighLow(52);
		return list.stream().skip(1)
				.sorted((i, j) -> i.get(mapKey.getDate()).toString().compareTo(j.get(mapKey.getDate()).toString()))
				.map(x -> {
					
					Double close = (Double) x.get(mapKey.getClose());
					// SMA
					for (SimpleMovingAverage sma : smaList) {
						sma.add((Double) close);
						x.put(sma.getPrefix() + mapKey.getSuffixMA(), sma.getAverage());
						x.put(sma.getPrefix() + mapKey.getSuffixSum(), sma.getSum());
						x.put(sma.getPrefix() + mapKey.getSuffixFirst(), sma.getFirst());
						x.put(sma.getPrefix() + mapKey.getSuffixSize(), sma.getSize());
					}
					
					// Week High Low
					weekHighLow.add((Integer) x.get(mapKey.getYear()), (Integer) x.get(mapKey.getDayOfYear()), x.get(mapKey.getDate()).toString(),
							close);
					Double week52High = weekHighLow.getHigh();
					Double week52Low = weekHighLow.getLow();
					x.put(mapKey.getNewHigh52W(), close.equals(week52High));
					x.put(mapKey.getNewLow52W(), close.equals(week52Low));
					x.put(mapKey.getHistoricalHigh(), weekHighLow.getHistoricalHigh());
					x.put(mapKey.getHistoricalHighDate(), weekHighLow.getHistoricalHighDate());
					x.put(mapKey.getHistoricalLow(), weekHighLow.getHistoricalLow());
					x.put(mapKey.getHistoricalLowDate(), weekHighLow.getHistoricalLowDate());
					x.put(weekHighLow.getPrefix() + mapKey.getSuffixHigh(), week52High);
					x.put(weekHighLow.getPrefix() + mapKey.getSuffixLow(), week52Low);
					
					return x;
				}).collect(Collectors.toList());
	}

	public List<Map<String, Object>> consolidate(String type, List<Map<String, Object>> list) {

		if (type.equals(mapValue.getWeekly())) {
			return consolidate(mapValue.getWeekly(), mapKey.getYearForWeek(), mapKey.getWeekOfYear(), list);
		} else if (type.equals(mapValue.getMonthly())) {
			return consolidate(mapValue.getMonthly(), mapKey.getYear(), mapKey.getMonth(), list);
		}
		return null;
	}

	public List<Map<String, Object>> consolidate(final String type, final String firstCriterion,
			final String secondCriterion, List<Map<String, Object>> list) {

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

			map.put(mapKey.getHigh(),
					x.stream().mapToDouble(a -> (Double) a.get(mapKey.getHigh())).max().getAsDouble());
			map.put(mapKey.getLow(), x.stream().mapToDouble(a -> (Double) a.get(mapKey.getLow())).min().getAsDouble());
			map.put(mapKey.getVolume(),
					x.stream().mapToLong(a -> Long.valueOf(a.get(mapKey.getVolume()).toString())).sum());

			return map;

		}).collect(Collectors.toList());

		return outputMapList;
	}
}