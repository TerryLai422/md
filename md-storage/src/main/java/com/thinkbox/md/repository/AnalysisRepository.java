package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Analysis;

public interface AnalysisRepository extends MongoRepository<Analysis, String>, AnalysisRepositoryCustom {

	List<Analysis> findByTicker(String ticker);
	
	List<Analysis> findByDate(String date);

	@Query(value= "{ ticker: {$eq: ?0},  date: {$gte: ?1}}", sort="{date:1}")
	List<Analysis> findByTickerAndDate(String ticker, String date);

	long countByTicker(String ticker);
	
	@Query(value="{_id: { $regex: '.*@20220504-000000' }, $where: ?0}", fields= "{ _id: 1}")
	List<Analysis> countByCriterion(String criterion);

}