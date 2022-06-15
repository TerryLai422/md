package com.thinkbox.md.service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.thinkbox.md.config.MapKeyParameter;
import com.thinkbox.md.mapper.DataMapper;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.AnalysisETF;
import com.thinkbox.md.model.DailySummary;
import com.thinkbox.md.model.DailySummaryETF;
import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;
import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.model.TradeDate;
import com.thinkbox.md.repository.AnalysisETFRepository;
import com.thinkbox.md.repository.AnalysisRepository;
import com.thinkbox.md.repository.DailySummaryETFRepository;
import com.thinkbox.md.repository.DailySummaryRepository;
import com.thinkbox.md.repository.HistoricalRepository;
import com.thinkbox.md.repository.InstrumentRepository;
import com.thinkbox.md.repository.TradeDateRepository;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class StoreService {

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static Double DEFAULT_DOUBLE_VALUE = 0d;

	private final static String INSTRUMENT_TYPE_ETF = "ETF";

	private final static int OBJECT_TYPE_INSTRUMENT = 1;

	private final static int OBJECT_TYPE_HISTORICAL = 2;

	private final static int OBJECT_TYPE_ANALYSIS = 3;

	private final static int OBJECT_TYPE_TRADEDATE = 4;

	private final static int OBJECT_TYPE_DAILYSUMMARY = 5;

	@Autowired
	private DailySummaryRepository dailySummaryRepository;

	@Autowired
	private DailySummaryETFRepository dailySummaryETFRepository;

	@Autowired
	private TradeDateRepository tradeDateRepository;

	@Autowired
	private AnalysisRepository analysisRepository;

	@Autowired
	private AnalysisETFRepository analysisETFRepository;

	@Autowired
	private HistoricalRepository historicalRepository;

	@Autowired
	private InstrumentRepository instrumentRepository;

	@Autowired
	private DataMapper mapper;

	@Autowired
	private MapKeyParameter mapKey;

	private void saveDailySummary(String type, Map<String, Object> map, Runnable completeConsumer) {

		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			
			dailySummaryETFRepository.save(mapper.convertMapToDailySummaryETF(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
			
		} else {
			
			dailySummaryRepository.save(mapper.convertMapToDailySummary(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
			
		}
	}

	public void saveMapList(int objType, List<Map<String, Object>> list, Runnable completeConsumer) {

		if (objType == OBJECT_TYPE_INSTRUMENT) {

			List<Instrument> convertedList = list.stream().skip(1).map(mapper::convertMapToInstrument)
					.collect(Collectors.toList());
			instrumentRepository.saveAll(convertedList).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_HISTORICAL) {

			List<Historical> convertedList = list.stream().skip(1).map(mapper::convertMapToHistorical)
					.collect(Collectors.toList());
			historicalRepository.saveAll(convertedList).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_TRADEDATE) {

			List<TradeDate> convertedList = list.stream().skip(1).map(mapper::convertMapToTradeDate)
					.collect(Collectors.toList());
			tradeDateRepository.saveAll(convertedList).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_ANALYSIS) {
			saveAnalysisList(list, completeConsumer);
		} else {
			log.info("Cannot find the corresponding object type");
		}
	}

	private void saveAnalysisList(List<Map<String, Object>> list, Runnable completeConsumer) {

		final Map<String, Object> firstMap = list.get(0);
		final String ticker = firstMap.get(mapKey.getTicker()).toString();
		final Map<String, Object> inst = getInstrument(ticker);

		String type = inst.getOrDefault(mapKey.getType(), DEFAULT_STRING_VALUE).toString();
		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			List<AnalysisETF> convertedList = list.stream().skip(1).map(x -> {
				x.put(mapKey.getInst(), inst);
				return mapper.convertMapToAnalysisETF(x);
			}).collect(Collectors.toList());
			analysisETFRepository.saveAll(convertedList).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
		} else {
			List<Analysis> convertedList = list.stream().skip(1).map(x -> {
				x.put(mapKey.getInst(), inst);
				return mapper.convertMapToAnalysis(x);
			}).collect(Collectors.toList());
			analysisRepository.saveAll(convertedList).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
		}
	}

	private void saveAnalysis(String type, Map<String, Object> map, Runnable completeConsumer) {

		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			
			analysisETFRepository.save(mapper.convertMapToAnalysisETF(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
			
		} else {
			
			analysisRepository.save(mapper.convertMapToAnalysis(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);
			
		}
	}

	public void saveMap(int objType, String type, Map<String, Object> map, Runnable completeConsumer) {
		if (objType == OBJECT_TYPE_INSTRUMENT) {

			instrumentRepository.save(mapper.convertMapToInstrument(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_HISTORICAL) {

			historicalRepository.save(mapper.convertMapToHistorical(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_TRADEDATE) {

			tradeDateRepository.save(mapper.convertMapToTradeDate(map)).subscribe(s -> {
			}, (e) -> {
			}, completeConsumer);

		} else if (objType == OBJECT_TYPE_ANALYSIS) {

			saveAnalysis(type, map, completeConsumer);

		} else if (objType == OBJECT_TYPE_DAILYSUMMARY) {

			saveDailySummary(type, map, completeConsumer);

		} else {
			log.info("Cannot find the corresponding object type");
		}
	}

	public Map<String, Object> getInstrument(final String ticker) {

		Instrument instrument = instrumentRepository.findByTicker(ticker).blockFirst();
		if (instrument != null) {
			return instrument.getOthers();
		}
		return null;
	}

	public List<Map<String, Object>> getInstrumentMapList(final String subExch, final String type,
			final String ticker) {
		List<Map<String, Object>> outList = null;
		if (!subExch.equals(DEFAULT_STRING_VALUE)) {
			if (!type.equals(DEFAULT_STRING_VALUE)) {
				if (!ticker.equals(DEFAULT_STRING_VALUE)) {

					List<Instrument> list = instrumentRepository.findBySubExchAndTypeAndTicker(subExch, type, ticker)
							.collectList().block();
					outList = list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

				} else {

					List<Instrument> list = instrumentRepository.findBySubExchAndType(subExch, type).collectList()
							.block();
					outList = list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

				}
			} else {

				List<Instrument> list = instrumentRepository.findBySubExch(subExch).collectList().block();
				outList = list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

			}
		}
		return outList;
	}

	public List<Map<String, Object>> getHistoricalTotalFromInstrument(final String subExch, int limit) {

		List<Instrument> list = instrumentRepository.findBySubExchAndTotal(subExch, limit).collectList().block();
//
//		return instruments.stream().filter(x -> x.getHistoricalTotal() <= limit).map(x -> x.getOthers()).collect(Collectors.toList());

		return list.stream().map(x -> x.getOthers()).collect(Collectors.toList());

	}

	public Flux<Map<String, Object>> getHistoricalMapFlux(final String ticker, final String date) {
		Flux<Map<String, Object>> flux = null;
		if (date.equals(DEFAULT_STRING_VALUE)) {
			flux = getHistoricalFlux(ticker);
		} else {
			flux = getHistoricalFlux(ticker, date);
		}
		return flux;
	}

	private Flux<Map<String, Object>> getHistoricalFlux(final String ticker) {

		return historicalRepository.findByTicker(ticker).map(x -> x.getOthers());
	}

	private Flux<Map<String, Object>> getHistoricalFlux(final String ticker, String date) {

		return historicalRepository.findByTickerAndDate(ticker, date).map(x -> x.getOthers());
	}

	public Map<String, Object> getHistoricalSummary(final String ticker) {

		HistoricalSummary summary = null;
		if (getHistoricalsTotal(ticker) > 0) {
			Flux<HistoricalSummary> flux = historicalRepository.getSummary(ticker);
			summary = flux.blockFirst();
		}

		return mapper.convertHistoricalSummaryToMap(summary);
	}

	public Map<String, Object> getHistoricalSummaryFromAllRecords(final String ticker) {

		List<Historical> list = historicalRepository.findByTicker(ticker).collectList().block();

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

		Mono<Long> count = historicalRepository.countByTicker(ticker);
		return count.block();

	}

	public String getHistoricalDate(final String ticker, Double close) {

		Flux<Historical> historicals = historicalRepository.findByTickerAndClose(ticker, close);

		Long count = historicals.count().block();
		if (count > 0) {
			Historical historical = historicals.blockFirst();
			return historical.getDate();
		}
		return DEFAULT_STRING_VALUE;
	}

	public List<Map<String, Object>> getTradeDateList() {
		Long start = System.currentTimeMillis();
		log.info("Start to get tradedate list");

		List<TradeDate> list = tradeDateRepository.findAll().collectList().block();
		System.out.println("Size:" + list.size());
		Long end = System.currentTimeMillis();
		log.info("Total time for getting tradedate list:" + (end - start));
		return list.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getTradeDateGreaterThanList(String date) {
		Long start = System.currentTimeMillis();
		log.info("Start to get tradedate list");

		List<TradeDate> list = tradeDateRepository.findByDateGreaterThan(date).collectList().block();

		Long end = System.currentTimeMillis();
		log.info("Total time for getting tradedate list:" + (end - start));
		return list.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}

	public List<Map<String, Object>> getTradeDateList(String date) {
		Long start = System.currentTimeMillis();
		log.info("Start to get tradedate list");

		List<TradeDate> list = tradeDateRepository.findByDate(date).collectList().block();

		Long end = System.currentTimeMillis();
		log.info("Total time for getting tradedate list:" + (end - start));
		return list.stream().map(mapper::convertTradeDateToMap).collect(Collectors.toList());
	}

	public Flux<Map<String, Object>> getAnalysisMapFluxByTickerAndDate(String type, String ticker, String date) {

		Flux<Map<String, Object>> flux = null;
		if (ticker.equals(DEFAULT_STRING_VALUE)) {
			flux = getAnalysisFluxByDate(type, date);
		} else {
			if (date.equals(DEFAULT_STRING_VALUE)) {
				flux = getAnalysisFluxByTicker(type, ticker);

			} else {
				flux = getAnalysisFluxByTickerAndDate(type, ticker, date);
			}
		}
		return flux;
	}

	private Flux<Map<String, Object>> getAnalysisFluxByTicker(String type, String ticker) {

		Flux<Map<String, Object>> outList = null;

		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			Flux<AnalysisETF> flux = analysisETFRepository.findByTicker(ticker);
			outList = flux.map(mapper::convertAnalysisToMap);
		} else {
			Flux<Analysis> flux = analysisRepository.findByTicker(ticker);
			outList = flux.map(mapper::convertAnalysisToMap);
		}

		return outList;
	}

	private Flux<Map<String, Object>> getAnalysisFluxByTickerAndDate(String type, String ticker, String date) {

		Flux<Map<String, Object>> outList = null;

		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			Flux<AnalysisETF> flux = analysisETFRepository.findByTickerAndDate(ticker, date);
			outList = flux.map(mapper::convertAnalysisToMap);
		} else {
			Flux<Analysis> flux = analysisRepository.findByTickerAndDate(ticker, date);
			outList = flux.map(mapper::convertAnalysisToMap);
		}
		return outList;
	}

	private Flux<Map<String, Object>> getAnalysisFluxByDate(String type, String date) {
		Long start = System.currentTimeMillis();
		log.info("Start to get analysis list by trade date");

		Flux<Map<String, Object>> outList = null;

		if (type.equals(INSTRUMENT_TYPE_ETF)) {
			Flux<AnalysisETF> flux = analysisETFRepository.findByDate(date);
			outList = flux.map(mapper::convertAnalysisToMap);
		} else {
			Flux<Analysis> flux = analysisRepository.findByDate(date);
			outList = flux.map(mapper::convertAnalysisToMap);
		}

		Long end = System.currentTimeMillis();
		log.info("Total time for getting analysis list by trade date:" + (end - start));

		return outList;
	}

	public long updateAnalysisField(String ticker, String date, String name, Object value) {
		return analysisRepository.countByCriterion(date, name);
	}

	public Flux<Map<String, Object>> getDateMapList(String date) {
		Long start = System.currentTimeMillis();
		log.info("Start to get dates");

		Flux<Map<String, Object>> outFlux = null;
		Flux<TradeDate> flux = null;

		if (date.equals(DEFAULT_STRING_VALUE)) {
			flux = analysisRepository.getDates();
		} else {
			flux = analysisRepository.getDates(date);
		}
		outFlux = flux.map(mapper::convertTradeDateToMap);

		Long end = System.currentTimeMillis();
		log.info("Total time for getting summary:" + (end - start));

		return outFlux;
	}
}
