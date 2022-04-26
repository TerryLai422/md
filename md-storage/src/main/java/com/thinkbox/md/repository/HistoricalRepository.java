package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;

public interface HistoricalRepository extends MongoRepository<Historical, String> {

	@Query("{ 'ticker': {$exists: false} }")
	List<Historical> findAllNull();

	@Query("{ 'ticker': {$exists: false} }")
	List<Historical> findAllNull(Pageable pageable);

	List<Historical> findByTicker(String ticker);

	List<Historical> findBySymbol(String symbol);

	long countByTicker(String ticker);

	long countBySymbol(String symbol);

	@Aggregation(pipeline = { "{$match: {ticker:?0}}",
			"{$group: { _id : $ticker, total: {$sum:1}, firstDate : { $max : $date}}}" })
	List<HistoricalSummary> getSummary(String ticker);

}