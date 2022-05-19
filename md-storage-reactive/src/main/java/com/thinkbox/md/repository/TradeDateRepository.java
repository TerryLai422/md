package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.TradeDate;

import reactor.core.publisher.Flux;

public interface TradeDateRepository extends ReactiveMongoRepository<TradeDate, String> {

	Flux<TradeDate> findByDate(String date);

	Flux<TradeDate> findByDateGreaterThan(String date);
	
}