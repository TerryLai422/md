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

	long countByTicker(String ticker);

	@Aggregation(pipeline = { "{$match: {ticker:?0}}",
			"{$group: { _id : $ticker, total: {$sum: 1}, firstDate: { $min: $date}, lastDate: {$max: $date} }}" })
	List<HistoricalSummary> getSummary(String ticker);

}