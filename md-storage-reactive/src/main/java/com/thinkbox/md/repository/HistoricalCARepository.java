package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.HistoricalCA;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface HistoricalCARepository extends ReactiveMongoRepository<HistoricalCA, String> {

//	@Query("{ 'ticker': {$exists: false} }")
//	List<HistoricalCA> findAllNull();
	
//	@Query("{ 'ticker': {$exists: false} }")
//	List<HistoricalCA> findAllNull(Pageable pageable);

	@Query(value= "{ticker:{$eq:?0}}", sort="{date:1}")
	Flux<Historical> findByTicker(String ticker);
	
	Mono<Long> countByTicker(String ticker);
	
//	long countBySymbol(String symbol);

}