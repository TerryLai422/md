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
import com.thinkbox.md.component.AverageVolume;
import com.thinkbox.md.config.IndicatorProperties;
import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.config.MapValueParameter;
import java.util.function.Function;

@Component
public class EnrichService {

	public final static int OBJECT_TYPE_HISTORICAL = 1;

	public final static int OBJECT_TYPE_ANALYSIS = 2;

	private final Logger logger = LoggerFactory.getLogger(EnrichService.class);

	private final static String TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE = ".TO";

	private final static String TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE = ".V";

	private final static String TORONTO_STOCK_EXCHANGE = "TSX";

	private final static String TORONTO_STOCK_VENTURE_EXCHANGE = "TSXV";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String STRING_EMPTY_SPACE = "";

	private final static Character CHARACTER_DASH = '-';

	private final static Character CHARACTER_DOT = '.';

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

	private List<Indicator> getIndicators(int type) {
		List<Indicator> indicators = new ArrayList<>();

		if (type == OBJECT_TYPE_HISTORICAL) {
			List<SimpleMovingAverage> smaList = getSMAList();
			PreviousClose previousClose = new PreviousClose(mapKey, 0);
			WeekHighLow weekHighLow = new WeekHighLow(mapKey, 52);
			AverageVolume volumeAverage = new AverageVolume(mapKey, 50);
			AverageTrueRange averageTrueRange = new AverageTrueRange(mapKey, 14);

			indicators.add(previousClose);
			indicators.add(weekHighLow);
			indicators.add(volumeAverage);
			indicators.add(averageTrueRange);

			for (Indicator indicator : smaList) {
				indicators.add(indicator);
			}
		} else {
			OnBalanceVolume onBalanceVolume = new OnBalanceVolume(mapKey, 0);
			indicators.add(onBalanceVolume);
		}

		return indicators;
	}

	public List<Map<String, Object>> enrichList(List<Map<String, Object>> list, int type) {

		Map<String, Object> firstMap = list.get(0);

		final String date = firstMap.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();

		final List<Indicator> indicators = getIndicators(type);

		return list.stream().skip(1).map(x -> {

			try {
				if (x.get(mapKey.getDate()).toString().compareTo(date) > 0) {
//						logger.info("x:" + x.get(mapKey.getDate()).toString() + "  -  " + date + ":true");
					x.put(mapKey.getSave(), true);
				} else {
//						logger.info("x:" + x.get(mapKey.getDate()).toString() + "  -  " + date + ":false");
					x.put(mapKey.getSave(), false);
				}

				if (!x.containsKey(mapKey.getInd())) {
					x.put(mapKey.getInd(), new TreeMap<>());
				}

				for (Indicator indicator : indicators) {
					indicator.process(x);
				}
			} catch (RuntimeException e) {
				e.printStackTrace();
				System.out.println("X:" + x.toString());
				x.put(mapKey.getSave(), false);
			}
			return x;
		}).filter(x -> Boolean.valueOf(x.getOrDefault(mapKey.getSave(), false).toString()))
				.collect(Collectors.toList());

	}

	public List<Map<String, Object>> enrichExchange(List<Map<String, Object>> list) {

		Map<String, Object> first = list.get(0);
		final String exchange = (String) first.get(mapKey.getExchange());
		final String suffix = (exchange.equals(TORONTO_STOCK_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE
				: (exchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE
						: STRING_EMPTY_SPACE;
		final boolean neededSuffix = (exchange.equals(TORONTO_STOCK_EXCHANGE)
				|| exchange.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) ? true : false;

		List<Map<String, Object>> outputList = list.stream().skip(1).limit(1).map(x -> {
			String symbol = (String) x.get(mapKey.getSymbol());
			String ticker = symbol;

			if (neededSuffix) {
				long count = symbol.chars().filter(ch -> ch == CHARACTER_DOT).count();
				if (count == 0) {
					ticker = symbol + suffix;
				} else if (count == 1) {
					ticker = symbol.replace(CHARACTER_DOT, CHARACTER_DASH) + suffix;
				} else {
					ticker = DEFAULT_STRING_VALUE;
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

	public Map<String, Object> createDailySummary(List<Map<String, Object>> list) {

		Map<String, Object> firstMap = list.remove(0);

		System.out.println("FirstMap:" + firstMap);

		final String date = firstMap.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString();

		Map<String, Object> outMap = new TreeMap<>();

		outMap.put(mapKey.getDate(), date);

		outMap.put("all", getMaps(list));
		
		outMap.put("sma50-sma200", getMaps(getFilterList(list, "sma50", "sma200")));
		
		System.out.println("OUTMAP: " + outMap.toString());
		
		return outMap;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getFilterList(List<Map<String, Object>> list, String key1, String key2) {
		return list.stream().filter(x -> {
			Map<String, Object> y = (Map<String, Object>) x.get(mapKey.getInd());
			Double i = Double.valueOf(y.getOrDefault(key1, 0).toString());
			Double j = Double.valueOf(y.getOrDefault(key2, 0).toString());
			return i != 0 && j !=0 && i > j;
		}).collect(Collectors.toList());
	}
	
	private Map<String, Object> getMaps(List<Map<String, Object>> list) {
		Map<String, Object> map = new TreeMap<>();

		map.put(mapKey.getTotal(), list.size());

		Map<String, Long> sectorMap = consolidate(list, mapKey.getInst(), mapKey.getSector());
		map.put(mapKey.getSector(), sectorMap);

		Map<String, Long> exchMap = consolidate(list, mapKey.getInst(), mapKey.getSubExch());
		map.put(mapKey.getSubExch(), exchMap);

		Map<String, Long> indMap = consolidate(list, mapKey.getInst(), mapKey.getGroup(), mapKey.getYahooIndustry());
		map.put(mapKey.getIndustry(), indMap);

		return map;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Long> consolidate(List<Map<String, Object>> list, String key1, String key2) {
		Map<String, Long> map = list.stream().map(x -> {
			Map<String, Object> y = (Map<String, Object>) x.get(key1);
			return y.getOrDefault(key2, "").toString();
		}).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Long> consolidate(List<Map<String, Object>> list, String key1, String key2, String key3) {
		Map<String, Long> map = list.stream().map(x -> {
			Map<String, Object> y = (Map<String, Object>) x.get(key1);
			Map<String, Object> z = (Map<String, Object>) y.get(key2);
			return z.getOrDefault(key3, "").toString();
		}).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		return map;
	}

}