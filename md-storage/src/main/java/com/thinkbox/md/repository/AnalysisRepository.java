package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Analysis;

public interface AnalysisRepository extends MongoRepository<Analysis, String> {

	List<Analysis> findByTicker(String ticker);

	@Query(value= "{ ticker: {$eq: ?0},  date: {$gte: ?1}}", sort="{date:1}")
	List<Analysis> findByTickerAndDate(String ticker, String date);

	long countByTicker(String ticker);

}