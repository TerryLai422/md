package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalSummary;

public interface HistoricalRepository extends MongoRepository<Historical, String> {

	@Query("{ticker: {$exists:false}}")
	List<Historical> findAllNull();

	@Query("{ticker: {$exists:false}}")
	List<Historical> findAllNull(Pageable pageable);

	List<Historical> findByTicker(String ticker);

	@Query(value= "{ticker:{$eq:?0}, date:{$gte:?1}}", sort="{date:1}")
	List<Historical> findByTickerAndDate(String ticker, String date);

	long countByTicker(String ticker);

	@Aggregation(pipeline = { "{$match: {ticker:?0}}",
	"{$project:{_id:$ticker, closeDate: {$concat:[{$toString: $close}, '-', $date]}, date:$date, close:$close}}", 
	"{$group:{_id :$_id, total:{$sum:1}, firstDate:{$min:$date}, lastDate:{$max:$date}, high:{$max:$closeDate}, low:{$min:$closeDate}, lastP:{$last:$close}}}" })	
	List<HistoricalSummary> getSummary(String ticker);

}