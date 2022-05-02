package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.Instrument;

public interface InstrumentRepository extends MongoRepository<Instrument, String> {

	List<Instrument> findBySubExch(String subExch);
	
	List<Instrument> findBySubExchAndType(String subExch, String type);
	
	List<Instrument> findByTicker(String ticker);

}