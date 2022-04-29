package com.thinkbox.md.mapper;

import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.model.InstrumentCA;

@Component
public class DataMapper {

	@Autowired
	private MapKeyParameter mapKey;

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static double DEFAULT_DOUBLE_VALUE = 0d;

	private final static long DEFAULT_LONG_VALUE = 0l;

	public Historical convertMapToHistorical(Map<String, Object> map) {

		return convertMapToHistorical(map, new Historical());

	}

	private Historical convertMapToHistorical(Map<String, Object> map, Historical historical) {
		
		historical.setId(map.get(mapKey.getType()) + "-" + map.get(mapKey.getSymbol()) + "@" + map.get(mapKey.getDate())
				+ "-" + map.get(mapKey.getTime()));
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

	public Instrument convertMapToInstrument(Map<String, Object> map) {

		Instrument instrument = null;

		String subExchange = map.getOrDefault(mapKey.getSubExchange(), "-").toString();
		if (subExchange.equals("TSX") || subExchange.equals("TSXV")) {
			instrument = new InstrumentCA();
		} else {
			instrument = new Instrument();
		}
		instrument.setId(
				map.getOrDefault(mapKey.getSubExchange(), DEFAULT_STRING_VALUE) + "@" + map.get(mapKey.getTicker()));
		instrument.setSymbol((String) map.getOrDefault(mapKey.getSymbol(), DEFAULT_STRING_VALUE));
		instrument.setTicker((String) map.getOrDefault(mapKey.getTicker(), DEFAULT_STRING_VALUE));
		instrument.setName((String) map.getOrDefault(mapKey.getName(), DEFAULT_STRING_VALUE));
		instrument.setExchange((String) map.getOrDefault(mapKey.getExchange(), DEFAULT_STRING_VALUE));
		instrument.setSubExchange((String) map.getOrDefault(mapKey.getSubExchange(), DEFAULT_STRING_VALUE));
		instrument.setExchangeName((String) map.getOrDefault(mapKey.getExchangeName(), DEFAULT_STRING_VALUE));
		instrument.setCountry((String) map.getOrDefault(mapKey.getCountry(), DEFAULT_STRING_VALUE));
		instrument.setCurrency((String) map.getOrDefault(mapKey.getCurrency(), DEFAULT_STRING_VALUE));
		instrument.setIndustry((String) map.getOrDefault(mapKey.getIndustry(), DEFAULT_STRING_VALUE));
		instrument.setSector((String) map.getOrDefault(mapKey.getSector(), DEFAULT_STRING_VALUE));
		instrument.setType((String) map.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE));
		instrument.setBeta(Double.valueOf(map.getOrDefault(mapKey.getBeta(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument
				.setForwardPE(Double.valueOf(map.getOrDefault(mapKey.getForwardPE(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setSharesOutstanding(
				Long.valueOf(map.getOrDefault(mapKey.getSharesOutstanding(), DEFAULT_LONG_VALUE).toString()));
		instrument.setMarketCap(
				Long.valueOf(map.getOrDefault(mapKey.getSharesOutstanding(), DEFAULT_LONG_VALUE).toString()));
		instrument.setHistoricalTotal(
				Long.valueOf(map.getOrDefault(mapKey.getHistoricalTotal(), DEFAULT_LONG_VALUE).toString()));
		instrument.setHistoricalFirstDate(
				(String) map.getOrDefault(mapKey.getHistoricalFirstDate(), DEFAULT_STRING_VALUE));
		instrument
				.setHistoricalLastDate((String) map.getOrDefault(mapKey.getHistoricalLastDate(), DEFAULT_STRING_VALUE));
		instrument.setHistoricalHigh(
				Double.valueOf(map.getOrDefault(mapKey.getHistoricalHigh(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setHistoricalLow(
				Double.valueOf(map.getOrDefault(mapKey.getHistoricalLow(), DEFAULT_DOUBLE_VALUE).toString()));
		instrument.setHistoricalHighDate((String) map.getOrDefault(mapKey.getHistoricalHighDate(), DEFAULT_STRING_VALUE));
		instrument.setHistoricalLowDate((String) map.getOrDefault(mapKey.getHistoricalLowDate(), DEFAULT_STRING_VALUE));

		instrument.setOthers(map);

		return instrument;
	}

	public Map<String, Object> convertHistoricalSummaryToMap(HistoricalSummary summary) {

		Map<String, Object> map = new TreeMap<>();
		;

		if (summary != null) {
			map.put(mapKey.getTicker(), summary.getTicker());
			map.put(mapKey.getHistoricalTotal(), summary.getTotal());
			map.put(mapKey.getHistoricalFirstDate(), summary.getFirstDate());
			map.put(mapKey.getHistoricalLastDate(), summary.getLastDate());
			map.put(mapKey.getHistoricalHigh(), summary.getHigh());
			map.put(mapKey.getHistoricalLow(), summary.getLow());
		}
		return map;
	}
}