package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.Instrument;

import reactor.core.publisher.Flux;

public interface InstrumentRepository extends ReactiveMongoRepository<Instrument, String> {

	Flux<Instrument> findBySubExch(String subExch);
	
	@Query(value= "{subExch:{$eq:?0},  hTotal:{$lte:?1}}")
	Flux<Instrument> findBySubExchAndTotal(String subExch, int total);

	@Query(value= "{subExch:{$eq:?0},  type:{$eq:?1}}", sort="{ticker:1}")
	Flux<Instrument> findBySubExchAndType(String subExch, String type);

	@Query(value= "{subExch:{$eq:?0},  type:{$eq:?1}, ticker:{$gt:?2}}", sort="{ticker:1}")
	Flux<Instrument> findBySubExchAndTypeAndTicker(String subExch, String type, String ticker);

	Flux<Instrument> findByTicker(String ticker);

}