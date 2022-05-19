package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.HistoricalCA;

public interface HistoricalCARepository extends ReactiveMongoRepository<HistoricalCA, String> {

	@Query("{ 'ticker': {$exists: false} }")
	List<HistoricalCA> findAllNull();
	
//	@Query("{ 'ticker': {$exists: false} }")
//	List<HistoricalCA> findAllNull(Pageable pageable);

	List<HistoricalCA> findByTicker(String ticker);

	List<HistoricalCA> findBySymbol(String symbol);
	
	long countByTicker(String ticker);

	long countBySymbol(String symbol);

}