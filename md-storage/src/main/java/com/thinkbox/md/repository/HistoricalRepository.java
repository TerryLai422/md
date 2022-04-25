package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Historical;

public interface HistoricalRepository extends MongoRepository<Historical, String> {

	@Query("{ 'ticker': {$exists: false} }")
	List<Historical> findAllNull();
	
	@Query("{ 'ticker': {$exists: false} }")
	List<Historical> findAllNull(Pageable pageable);

	List<Historical> findByTicker(String ticker);

	List<Historical> findBySymbol(String symbol);
	
	long countByTicker(String ticker);

	long countBySymbol(String symbol);

}