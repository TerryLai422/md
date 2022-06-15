package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.TradeDate;

import reactor.core.publisher.Flux;

public interface AnalysisRepository extends ReactiveMongoRepository<Analysis, String>, AnalysisRepositoryCustom {

	@Query(value= "{ticker:{$eq:?0}}", sort="{date:1}")
	Flux<Analysis> findByTicker(String ticker);

	Flux<Analysis> findByDate(String date);

	@Query(value= "{ ticker: {$eq: ?0},  date: {$gte: ?1}}", sort="{date:1}")
	Flux<Analysis> findByTickerAndDate(String ticker, String date);

	long countByTicker(String ticker);
	
//	@Query(value="{_id: { $regex: '.*@20220504-000000' }, $where: ?0}", fields= "{ _id: 1}")
//	List<Analysis> countByCriterion(String criterion);
	
	@Aggregation(pipeline = { "{$group:{_id :$date, total:{$sum:1}}}, {$sort:{_id: 1}}" })	
	Flux<TradeDate> getDates(); 

	@Aggregation(pipeline = { "{$match:{date:{$gte:?0}}}","{$group:{_id :$date, total:{$sum:1}}}","{$sort:{_id: 1}}" })	
	Flux<TradeDate> getDates(String date); 
}