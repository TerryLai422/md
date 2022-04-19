package com.thinkbox.md.mapper;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.Instrument;

@Component
public class DataMapper {

	@Autowired
	private MapKeyParameter mapKey;

	public Historical convertHistorical(Map<String, Object> map) {

		Historical historical = new Historical();

		historical
				.setId(map.get(mapKey.getType()) + "-" + map.get(mapKey.getSymbol()) + "@" + map.get(mapKey.getDate()));
		historical.setType((String) map.get(mapKey.getType()));
		historical.setSymbol((String) map.get(mapKey.getSymbol()));
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
		historical.setVolume((Long) map.get(mapKey.getVolume()));
		historical.setOthers(map);

		return historical;
	}

	public Instrument convertInstrument(Map<String, Object> map) {
		System.out.println("Mapper:" + map.toString());
		Instrument instrument = new Instrument();

		instrument.setId(map.getOrDefault(mapKey.getExchange(), "-") + "@" + map.get(mapKey.getSymbol()));
		instrument.setSymbol((String) map.getOrDefault(mapKey.getSymbol(), "-"));
		instrument.setName((String) map.getOrDefault(mapKey.getName(), "-"));
		instrument.setExchange((String) map.getOrDefault(mapKey.getExchange(), "-"));
		instrument.setExchangeName((String) map.getOrDefault(mapKey.getExchangeName(), "-"));
		instrument.setCountry((String) map.getOrDefault(mapKey.getCountry(), "-"));
		instrument.setCurrency((String) map.getOrDefault(mapKey.getCurrency(), "-"));
		instrument.setIndustry((String) map.getOrDefault(mapKey.getIndustry(), "-"));
		instrument.setSector((String) map.getOrDefault(mapKey.getSector(), "-"));
		instrument.setType((String) map.getOrDefault(mapKey.getType(), "-"));
		instrument.setBeta((Double) map.getOrDefault(mapKey.getBeta(), 0d));
		instrument.setForwardPE((Double) map.getOrDefault(mapKey.getForwardPE(), 0d));
		instrument.setSharesOutstanding(Long.valueOf(map.getOrDefault(mapKey.getSharesOutstanding(), 0).toString()));
		instrument.setMarketCap(Long.valueOf(map.getOrDefault(mapKey.getSharesOutstanding(), 0).toString()));
		instrument.setOthers(map);

		return instrument;
	}
}