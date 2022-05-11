package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.TradeDate;

public interface TradeDateRepository extends MongoRepository<TradeDate, String> {

	List<TradeDate> findByDate(String date);

	List<TradeDate> findByDateGreaterThan(String date);
	
}