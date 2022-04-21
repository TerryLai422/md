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

import com.thinkbox.md.component.AverageTrueRange;
import com.thinkbox.md.component.Indicator;
import com.thinkbox.md.component.OnBalanceVolume;
import com.thinkbox.md.component.PreviousClose;
import com.thinkbox.md.component.SimpleMovingAverage;
import com.thinkbox.md.component.WeekHighLow;
import com.thinkbox.md.component.VolumeAverage;
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
			list.add(new SimpleMovingAverage(mapKey, i));
		}
		return list;
	}

	private List<Indicator> getIndicators() {
		List<SimpleMovingAverage> smaList = getSMAList();
		PreviousClose previousClose = new PreviousClose(mapKey, 0);
		WeekHighLow weekHighLow = new WeekHighLow(mapKey, 52);
		VolumeAverage volumeAverage = new VolumeAverage(mapKey, 50);
		OnBalanceVolume onBalanceVolume = new OnBalanceVolume(mapKey, 0);
		AverageTrueRange averageTrueRange = new AverageTrueRange(mapKey, 14);
		
		List<Indicator> indicators = Arrays.asList();
		indicators.add(previousClose);
		indicators.add(weekHighLow);
		indicators.add(volumeAverage);
		indicators.add(onBalanceVolume);
		indicators.add(averageTrueRange);

		for (Indicator indicator: smaList) {
			indicators.add(indicator);			
		}
		return indicators;
	}

	public List<Map<String, Object>> enrichHistorical(List<Map<String, Object>> list) {

		final List<Indicator> indicators = getIndicators();
		
		return list.stream().skip(1)
				.sorted((i, j) -> i.get(mapKey.getDate()).toString().compareTo(j.get(mapKey.getDate()).toString()))
				.map(x -> {

					for (Indicator indicator: indicators) {
						indicator.process(x);
					}
					return x;
				}).collect(Collectors.toList());
	}

	public List<Map<String, Object>> enrichExchange(List<Map<String, Object>> list) {

		Map<String, Object> first = list.get(0);
		final String exchange = (String) first.get(mapKey.getExchange());
		final String suffix = (exchange.equals("TSX")) ? ".TO" : (exchange.equals("TSXV")) ? ".V" : "";
		final boolean neededSuffix = (exchange.equals("TSX") || exchange.equals("TSXV")) ? true : false;

		List<Map<String, Object>> outputList = list.stream().skip(1).map(x -> {
			String symbol = (String) x.get(mapKey.getSymbol());
			String ticker = symbol;
			
			if (neededSuffix) {
				long count = symbol.chars().filter(ch -> ch == '.').count();
				if (count == 0) {
					ticker = symbol + suffix;
				} else if (count == 1) {
					ticker = symbol.replace('.', '-') + suffix;
				} else {
					ticker = "-";
				}
			}

			x.put(mapKey.getTicker(), ticker);
			
			return x;
		}).collect(Collectors.toList());
		
		Map<String, Object> index = new TreeMap<String, Object>();

		index.put(mapKey.getTotal(), Long.valueOf(outputList.size()));

		outputList.add(0, index);
		
		return outputList;
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