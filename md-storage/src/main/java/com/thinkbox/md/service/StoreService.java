package com.thinkbox.md.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.repository.AnalysisRepository;
import com.thinkbox.md.repository.HistoricalRepository;
import com.thinkbox.md.repository.InstrumentRepository;

@Component
public class StoreService {

	private final static String DEFAULT_STRING_VALUE = "-";
	
	private final static Double DEFAULT_DOUBLE_VALUE = 0d;
	
	@Autowired
	private AnalysisRepository analysisRepository;

	@Autowired
	private HistoricalRepository historicalRepository;

	@Autowired
	private InstrumentRepository instrumentRepository;

	@Autowired
	private DataMapper mapper;

	@Autowired
	private MapKeyParameter mapKey;

	public void saveAnalysisList(List<Map<String, Object>> list) {

		final Map<String, Object> firstMap = list.get(0);
		final String ticker = firstMap.get(mapKey.getTicker()).toString();
		final Map<String, Object> inst = getInstrument(ticker);

		List<Analysis> convertedList = list.stream().skip(1).map(x -> {
			return mapper.convertMapToAnalysis(x, inst);
		}).collect(Collectors.toList());

		analysisRepository.saveAll(convertedList);

	}

	public void saveHistoricalList(List<Map<String, Object>> list) {

		List<Historical> convertedList = list.stream().skip(1).map(mapper::convertMapToHistorical)
				.collect(Collectors.toList());

		historicalRepository.saveAll(convertedList);

	}

	public void saveHistorical(Map<String, Object> map) {

		Historical historical = mapper.convertMapToHistorical(map);

		historicalRepository.save(historical);

	}

	public void saveInstrumentList(List<Map<String, Object>> list) {

		List<Instrument> convertedList = list.stream().skip(1).map(mapper::convertMapToInstrument)
				.collect(Collectors.toList());

		instrumentRepository.saveAll(convertedList);

	}

	public void saveInstrument(Map<String, Object> map) {

		Instrument instrument = mapper.convertMapToInstrument(map);

		instrumentRepository.save(instrument);

	}

	public Map<String, Object> getInstrument(final String ticker) {

		List<Instrument> instruments = instrumentRepository.findByTicker(ticker);
		if (instruments != null && instruments.size() > 0) {
			return instruments.get(0).getOthers();
		}
		return null;
	}

	public List<Map<String, Object>> getInstruments(final String subExch) {

		List<Instrument> instruments = instrumentRepository.findBySubExch(subExch);

		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getInstruments(final String subExch, final String type) {

		List<Instrument> instruments = instrumentRepository.findBySubExchAndType(subExch, type);

		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getInstruments(final String subExch, final String type, final String ticker) {

		List<Instrument> instruments = instrumentRepository.findBySubExchAndTypeAndTicker(subExch, type, ticker);

		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricalTotalFromInstrument(final String subExch, int limit) {

		List<Instrument> instruments = instrumentRepository.findBySubExchAndTotal(subExch, limit);
//
//		return instruments.stream().filter(x -> x.getHistoricalTotal() <= limit).map(x -> x.getOthers()).collect(Collectors.toList());

		return instruments.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricals(int page) {

		PageRequest pageRequest = PageRequest.of(page, 2000);

		List<Historical> historicals = historicalRepository.findAllNull(pageRequest);

		return historicals.stream().limit(2000).map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricals(final String ticker) {

		List<Historical> historicals = historicalRepository.findByTicker(ticker);

		return historicals.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricals(final String ticker, String date) {

		List<Historical> historicals = Arrays.asList();

		historicals = historicalRepository.findByTickerAndDate(ticker, date);

		return historicals.stream().map(x -> x.getOthers()).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getHistoricals(final String ticker, long limit) {

		List<Historical> historicals = Arrays.asList();

		if (historicalRepository.countByTicker(ticker) <= limit) {
			historicals = historicalRepository.findByTicker(ticker);
		}

		return historicals.stream().sorted((i, j) -> i.getDate().toString().compareTo(j.getDate().toString()))
				.map(x -> x.getOthers()).collect(Collectors.toList());
	}

	public Map<String, Object> getHistoricalSummary(final String ticker) {

		HistoricalSummary summary = null;
		if (historicalRepository.countByTicker(ticker) > 0) {
			List<HistoricalSummary> list = historicalRepository.getSummary(ticker);
			summary = list.get(0);
		}

		return mapper.convertHistoricalSummaryToMap(summary);
	}

	public Map<String, Object> getHistoricalSummaryFromAllRecords(final String ticker) {

		List<Historical> list = historicalRepository.findByTicker(ticker);

		Map<String, Object> x = new TreeMap<>();
		int size = list.size();
		x.put(mapKey.getHTotal(), size);
		if (size > 0) {
			Historical historical = list.get(0);
			x.put(mapKey.getHFirstD(), historical.getDate());
			historical = list.get(list.size() - 1);
			x.put(mapKey.getHLastD(), historical.getDate());
			x.put(mapKey.getLastP(), historical.getClose());
		} else {
			x.put(mapKey.getHFirstD(), DEFAULT_STRING_VALUE);
			x.put(mapKey.getHLastD(), DEFAULT_STRING_VALUE);
			x.put(mapKey.getLastP(), DEFAULT_DOUBLE_VALUE);
		}
		
		Optional<Historical> oHighest = list.stream().max(Comparator.comparing(Historical::getClose));
		if (oHighest != null && oHighest.isPresent()) {
			Historical hHigh = oHighest.get();
			x.put(mapKey.getHHighD(), hHigh.getDate());
			x.put(mapKey.getHHigh(), hHigh.getClose());
		}

		Optional<Historical> oLowest = list.stream().min(Comparator.comparing(Historical::getClose));
		if (oLowest != null && oLowest.isPresent()) {
			Historical hLow = oLowest.get();
			x.put(mapKey.getHLowD(), hLow.getDate());
			x.put(mapKey.getHLow(), hLow.getClose());
		}
		return x;
	}
	
	public Long getHistoricalsTotal(final String ticker) {

		return historicalRepository.countByTicker(ticker);

	}
	
	public String getDate(final String ticker, Double close) {

		List<Historical> historicals = Arrays.asList();

		historicals = historicalRepository.findByTickerAndClose(ticker, close);

		if (historicals.size() > 0) {
			Historical historical = historicals.get(0);
			return historical.getDate();
		}
		return DEFAULT_STRING_VALUE;
	}

	public int countByCriterion(String criterion) {

		List<Analysis> list = analysisRepository.countByCriterion(criterion);
		return list.size();

	}
	
	public long updateAnalysisField(String ticker, String date, String name, Object value) {
		return analysisRepository.countByCriterion(date, name);
	}
}
