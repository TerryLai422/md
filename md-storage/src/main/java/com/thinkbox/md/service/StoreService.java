package com.thinkbox.md.service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.DailySummary;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.model.TradeDate;
import com.thinkbox.md.repository.AnalysisRepository;
import com.thinkbox.md.repository.DailySummaryRepository;
import com.thinkbox.md.repository.HistoricalRepository;
import com.thinkbox.md.repository.InstrumentRepository;
import com.thinkbox.md.repository.TradeDateRepository;

@Component
public class StoreService {

	private final Logger logger = LoggerFactory.getLogger(StoreService.class);

	private final static String DEFAULT_STRING_VALUE = "-";
	
	private final static Double DEFAULT_DOUBLE_VALUE = 0d;

	@Autowired
	private DailySummaryRepository dailySummaryRepository;

	@Autowired
	private TradeDateRepository tradeDateRepository;

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

	public void saveDailySummary(Map<String, Object> map) {

		DailySummary dailySummary = mapper.convertMapToDailySummary(map);

		dailySummaryRepository.save(dailySummary);

	}
	
	public void saveTradeDateList(List<Map<String, Object>> list) {

		List<TradeDate> convertedList = list.stream().skip(1).map(mapper::convertMapToTradeDate)
				.collect(Collectors.toList());

		tradeDateRepository.saveAll(convertedList);

	}
	
	public void saveAnalysisList(List<Map<String, Object>> list) {

		final Map<String, Object> firstMap = list.get(0);
		final String ticker = firstMap.get(mapKey.getTicker()).toString();
		final Map<String, Object> inst = getInstrument(ticker);

		List<Analysis> convertedList = list.stream().skip(1).map(x -> {
			x.put(mapKey.getInst(), inst);
			return mapper.convertMapToAnalysis(x);
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

	public void saveAnalysis(Map<String, Object> map) {

		Analysis analysis = mapper.convertMapToAnalysis(map);

		analysisRepository.save(analysis);

	}
	
	public void saveTradeDate(Map<String, Object> map) {

		TradeDate tradeDate = mapper.convertMapToTradeDate(map);

		tradeDateRepository.save(tradeDate);

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

	public List<Map<String, Object>> getInstrumentList(final String subExch) {

		List<Instrument> list = instrumentRepository.findBySubExch(subExch);

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getInstrumentList(final String subExch, final String type) {

		List<Instrument> list = instrumentRepository.findBySubExchAndType(subExch, type);

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getInstrumentList(final String subExch, final String type, final String ticker) {

		List<Instrument> list = instrumentRepository.findBySubExchAndTypeAndTicker(subExch, type, ticker);

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricalTotalFromInstrument(final String subExch, int limit) {

		List<Instrument> list = instrumentRepository.findBySubExchAndTotal(subExch, limit);
//
//		return instruments.stream().filter(x -> x.getHistoricalTotal() <= limit).map(x -> x.getOthers()).collect(Collectors.toList());

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricalList(int page) {

		PageRequest pageRequest = PageRequest.of(page, 2000);

		List<Historical> historicals = historicalRepository.findAllNull(pageRequest);

		return historicals.stream().limit(2000).map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricalList(final String ticker) {

		List<Historical> list = historicalRepository.findByTicker(ticker);

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public List<Map<String, Object>> getHistoricalList(final String ticker, String date) {

		List<Historical> list = historicalRepository.findByTickerAndDate(ticker, date);

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getHistoricalList(final String ticker, long limit) {

		List<Historical> list = Arrays.asList();

		if (historicalRepository.countByTicker(ticker) <= limit) {
			list = historicalRepository.findByTicker(ticker);
		}

		return list.stream().sorted((i, j) -> i.getDate().toString().compareTo(j.getDate().toString()))
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
	
	public String getHistoricalDate(final String ticker, Double close) {

		List<Historical> historicals = Arrays.asList();

		historicals = historicalRepository.findByTickerAndClose(ticker, close);

		if (historicals.size() > 0) {
			Historical historical = historicals.get(0);
			return historical.getDate();
		}
		return DEFAULT_STRING_VALUE;
	}

	public List<Map<String, Object>> getAnalysisList(String ticker) {
	
		List<Analysis> list = analysisRepository.findByTicker(ticker);

		return list.stream().map(mapper::convertAnalysisToMap).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getTradeDateList(String date) {
		Long start = System.currentTimeMillis();
		logger.info("Start to get tradedate list");

		List<TradeDate> List = tradeDateRepository.findByDate(date);
		
		Long end = System.currentTimeMillis();
		logger.info("Total time for getting tradedate list:" + (end - start));
		return List.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}

	
	public List<Map<String, Object>> getAnalysisList(String ticker, String date) {
		
		List<Analysis> list = analysisRepository.findByTickerAndDate(ticker, date);

		return list.stream().map(mapper::convertAnalysisToMap).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getAnalysisListByDate(String date) {
		Long start = System.currentTimeMillis();
		logger.info("Start to get analysis list by trade date");

		List<Analysis> list = analysisRepository.findByDate(date);

		Long end = System.currentTimeMillis();
		logger.info("Total time for getting analysis list by trade date:" + (end - start));

		return list.stream().map(mapper::convertAnalysisToMap).collect(Collectors.toList());
	}
	
	public long updateAnalysisField(String ticker, String date, String name, Object value) {
		return analysisRepository.countByCriterion(date, name);
	}

	public List<Map<String, Object>> getDates() {
		Long start = System.currentTimeMillis();
		logger.info("Start to get dates");
		List<TradeDate> list = analysisRepository.getDates();
		Long end = System.currentTimeMillis();
		logger.info("Total time for getting summary:" + (end - start));
		return list.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}
	
	public List<Map<String, Object>> getDates(String date) {
		Long start = System.currentTimeMillis();
		logger.info("Start to get dates:" + date);
		List<TradeDate> list = analysisRepository.getDates(date);
		Long end = System.currentTimeMillis();
		logger.info("Total time for getting summary:" + (end - start));
		return list.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}
	
	public int countByCriterion(String criterion) {

		List<Analysis> list = analysisRepository.countByCriterion(criterion);
		return list.size();

	}
}
