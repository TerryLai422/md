package com.thinkbox.md.mapper;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.AnalysisETF;
import com.thinkbox.md.model.DailySummary;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.model.InstrumentCA;
import com.thinkbox.md.model.TradeDate;

@Component
public class DataMapper {

	@Autowired
	private MapKeyParameter mapKey;

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static double DEFAULT_DOUBLE_VALUE = 0d;

	private final static long DEFAULT_LONG_VALUE = 0l;

	private final static int DEFAULT_INTEGER_VALUE = 0;

	private final static String TORONTO_STOCK_EXCHANGE = "TSX";

	private final static String TORONTO_STOCK_VENTURE_EXCHANGE = "TSXV";

	private final static String STRING_DASH = "-";

	private final static String STRING_AT_SYMBOL = "@";

	
	public Map<String, Object> convertAnalysisToMap(Analysis analysis) {

		Map<String, Object> map = new TreeMap<>();
		
		map.put(mapKey.getInst(), analysis.getInst());
		map.put(mapKey.getInd(), analysis.getInd());
		map.put(mapKey.getInterval(), analysis.getInterval());
		map.put(mapKey.getSymbol(), analysis.getSymbol());
		map.put(mapKey.getTicker(), analysis.getTicker());
		map.put(mapKey.getDate(), analysis.getDate());
		map.put(mapKey.getTime(), analysis.getTime());
		map.put(mapKey.getYear(), analysis.getYear());
		map.put(mapKey.getMonth(), analysis.getMonth());
		map.put(mapKey.getDay(), analysis.getDay());
		map.put(mapKey.getWeekOfYear(), analysis.getWeekOfYear());
		map.put(mapKey.getDayOfYear(), analysis.getDayOfYear());
		map.put(mapKey.getDayOfWeek(), analysis.getDayOfWeek());
		map.put(mapKey.getOpen(), analysis.getOpen());
		map.put(mapKey.getHigh(), analysis.getHigh());
		map.put(mapKey.getLow(), analysis.getLow());
		map.put(mapKey.getClose(), analysis.getClose());
		map.put(mapKey.getAdjClose(), analysis.getAdjClose());
		map.put(mapKey.getVolume(), analysis.getVolume());

		return map;
	}

	@SuppressWarnings("unchecked")
	public AnalysisETF convertMapToAnalysisETF(Map<String, Object> map) {

		Map<String, Object> inst = (Map<String, Object>) map.remove(mapKey.getInst());
		Map<String, Object> ind = (Map<String, Object>) map.remove(mapKey.getInd());
		map.remove(mapKey.getSave());
		AnalysisETF analysis = (AnalysisETF) convertMapToHistorical(map, new AnalysisETF());						
		analysis.setInd(ind);
		analysis.setInst(inst);
		return analysis;

	}
	
	@SuppressWarnings("unchecked")
	public Analysis convertMapToAnalysis(Map<String, Object> map) {

		Map<String, Object> inst = (Map<String, Object>) map.remove(mapKey.getInst());
		Map<String, Object> ind = (Map<String, Object>) map.remove(mapKey.getInd());
		map.remove(mapKey.getSave());
		Analysis analysis = (Analysis) convertMapToHistorical(map, new Analysis());			
		analysis.setInd(ind);
		analysis.setInst(inst);
		return analysis;

	}

	public Historical convertMapToHistorical(Map<String, Object> map) {

		return convertMapToHistorical(map, new Historical());

	}

	private Historical convertMapToHistorical(Map<String, Object> map, Historical historical) {

		historical.setId(map.get(mapKey.getInterval()) + STRING_DASH + map.get(mapKey.getSymbol()) + STRING_AT_SYMBOL
				+ map.get(mapKey.getDate()) + STRING_DASH + map.get(mapKey.getTime()));
		historical.setInterval((String) map.get(mapKey.getInterval()));
		historical.setSymbol((String) map.get(mapKey.getSymbol()));
		historical.setTicker((String) map.getOrDefault(mapKey.getTicker(), map.get(mapKey.getSymbol())));
		historical.setDate((String) map.get(mapKey.getDate()));
		historical.setTime((String) map.get(mapKey.getTime()));
		historical.setYear((Integer) map.get(mapKey.getYear()));
		historical.setMonth((Integer) map.get(mapKey.getMonth()));
		historical.setDay((Integer) map.get(mapKey.getDay()));
		historical.setWeekOfYear((Integer) map.get(mapKey.getWeekOfYear()));
		historical.setDayOfYear((Integer) map.get(mapKey.getDayOfYear()));
		historical.setDayOfWeek((Integer) map.get(mapKey.getDayOfWeek()));
		historical.setOpen((Double) map.get(mapKey.getOpen()));
		historical.setHigh((Double) map.get(mapKey.getHigh()));
		historical.setLow((Double) map.get(mapKey.getLow()));
		historical.setClose((Double) map.get(mapKey.getClose()));
		historical.setAdjClose((Double) map.get(mapKey.getAdjClose()));
		historical.setVolume(Long.valueOf(map.getOrDefault(mapKey.getVolume(), DEFAULT_LONG_VALUE).toString()));
		historical.setOthers(map);

		return historical;

	}

	@SuppressWarnings("unchecked")
	public Instrument convertMapToInstrument(Map<String, Object> map) {

		Instrument instrument = null;

		String subExch = map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE).toString();
		if (subExch.equals(TORONTO_STOCK_EXCHANGE) || subExch.equals(TORONTO_STOCK_VENTURE_EXCHANGE)) {
			instrument = new InstrumentCA();
		} else {
			instrument = new Instrument();
		}
		instrument.setId(map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE) + STRING_AT_SYMBOL
				+ map.get(mapKey.getTicker()));
		instrument.setSymbol((String) map.getOrDefault(mapKey.getSymbol(), DEFAULT_STRING_VALUE));
		instrument.setTicker((String) map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE));
		instrument.setName((String) map.getOrDefault(mapKey.getName(), DEFAULT_STRING_VALUE));
		instrument.setExchange((String) map.getOrDefault(mapKey.getExchange(), DEFAULT_STRING_VALUE));
		instrument.setSubExch((String) map.getOrDefault(mapKey.getSubExch(), DEFAULT_STRING_VALUE));
		instrument.setExchangeN((String) map.getOrDefault(mapKey.getExchangeN(), DEFAULT_STRING_VALUE));
		instrument.setCountry((String) map.getOrDefault(mapKey.getCountry(), DEFAULT_STRING_VALUE));
		instrument.setCurrency((String) map.getOrDefault(mapKey.getCurrency(), DEFAULT_STRING_VALUE));
		instrument.setSector((String) map.getOrDefault(mapKey.getSector(), DEFAULT_STRING_VALUE));
		instrument.setType((String) map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE));
		instrument.setBeta(Double.valueOf(map.getOrDefault(mapKey.getBeta(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setFPE(Double.valueOf(map.getOrDefault(mapKey.getFPE(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setPB(
				Double.valueOf(map.getOrDefault(mapKey.getPB(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setSharesO(Long.valueOf(map.getOrDefault(mapKey.getSharesO(), DEFAULT_LONG_VALUE).toString()));
		instrument.setMCap(Long.valueOf(map.getOrDefault(mapKey.getMCap(), DEFAULT_LONG_VALUE).toString()));
		instrument.setHTotal(Long.valueOf(map.getOrDefault(mapKey.getHTotal(), DEFAULT_LONG_VALUE).toString()));
		instrument.setHFirstD((String) map.getOrDefault(mapKey.getHFirstD(), DEFAULT_STRING_VALUE));
		instrument.setHLastD((String) map.getOrDefault(mapKey.getHLastD(), DEFAULT_STRING_VALUE));
		instrument.setHHigh(Double.valueOf(map.getOrDefault(mapKey.getHHigh(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setHLow(Double.valueOf(map.getOrDefault(mapKey.getHLow(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setHHighD((String) map.getOrDefault(mapKey.getHHighD(), DEFAULT_STRING_VALUE));
		instrument.setHLowD((String) map.getOrDefault(mapKey.getHLowD(), DEFAULT_STRING_VALUE));
		instrument
				.setLastP(Double.valueOf(map.getOrDefault(mapKey.getLastP(), DEFAULT_DOUBLE_VALUE).toString()));

		String industry = (String) map.getOrDefault(mapKey.getIndustry(), DEFAULT_STRING_VALUE);
		Object groupObject = map.get(mapKey.getGroup());
		Map<String, Object> groupMap = null;
		if (groupObject == null) {
			groupMap = new TreeMap<String, Object>();
		} else {
			groupMap = (Map<String, Object>) groupObject;
		}
		groupMap.put(mapKey.getYahooIndustry(), industry);
		map.put(mapKey.getGroup(), groupMap);
		instrument.setGroup(groupMap);
		instrument.setOthers(map);

		return instrument;

	}

	public Map<String, Object> convertHistoricalSummaryToMap(HistoricalSummary summary) {

		Map<String, Object> map = new TreeMap<>();
		;

		if (summary != null) {
			map.put(mapKey.getTicker(), summary.getTicker());
			map.put(mapKey.getHTotal(), summary.getTotal());
			map.put(mapKey.getLastP(), summary.getLastP());
			map.put(mapKey.getHFirstD(), summary.getFirstDate());
			map.put(mapKey.getHLastD(), summary.getLastDate());
			map.put(mapKey.getHHigh(), summary.getHigh());
			map.put(mapKey.getHLow(), summary.getLow());
		}
		return map;

	}

	public Map<String, Object> convertTradeDateToMap(TradeDate tradeDate) {

		Map<String, Object> map = new TreeMap<>();

		if (tradeDate != null) {
			map.put(mapKey.getDate(), tradeDate.getDate());
			map.put(mapKey.getTotal(), tradeDate.getTotal());
		}
		return map;

	}
	
	public TradeDate convertMapToTradeDate(Map<String, Object> map) {

		TradeDate tradeDate = new TradeDate();;

		tradeDate.setDate((String) map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE));
		tradeDate.setTotal(Integer.valueOf(map.getOrDefault(mapKey.getTotal(), DEFAULT_INTEGER_VALUE).toString()));

		return tradeDate;

	}

	public DailySummary convertMapToDailySummary(Map<String, Object> map) {

		System.out.println("MAP:" + map.toString());
		DailySummary dailySummary = new DailySummary();

		dailySummary.setDate(map.getOrDefault(mapKey.getDate(), DEFAULT_STRING_VALUE).toString());
		dailySummary.setMap(map);
		return dailySummary;

	}

}