package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.AnalysisETF;
import com.thinkbox.md.model.TradeDate;

public interface AnalysisETFRepository extends MongoRepository<AnalysisETF, String>, AnalysisRepositoryCustom {

	@Query(value= "{ticker:{$eq:?0}}", sort="{date:1}")
	List<AnalysisETF> findByTicker(String ticker);

	List<AnalysisETF> findByDate(String date);

	@Query(value= "{ ticker: {$eq: ?0},  date: {$gte: ?1}}", sort="{date:1}")
	List<AnalysisETF> findByTickerAndDate(String ticker, String date);

	long countByTicker(String ticker);
	
	@Query(value="{_id: { $regex: '.*@20220504-000000' }, $where: ?0}", fields= "{ _id: 1}")
	List<AnalysisETF> countByCriterion(String criterion);
	
	@Aggregation(pipeline = { "{$group:{_id :$date, total:{$sum:1}}}, {$sort:{_id: 1}}" })	
	List<TradeDate> getDates(); 

	@Aggregation(pipeline = { "{$match:{date:{$gte:?0}}}","{$group:{_id :$date, total:{$sum:1}}}","{$sort:{_id: 1}}" })	
	List<TradeDate> getDates(String date); 
}