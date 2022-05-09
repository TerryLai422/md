package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.TradeDate;

public interface TradeDateRepository extends MongoRepository<TradeDate, String> {

	@Query(value= "{date:{$exists:true}", sort="{date:1}")
	List<TradeDate> findAll();

	List<TradeDate> findByDate(String date);

}