package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.Instrument;

public interface InstrumentRepository extends MongoRepository<Instrument, String> {

	List<Instrument> findBySubExch(String subExch);
	
	@Query(value= "{subExch:{$eq:?0},  hTotal:{$lte:?1}}")
	List<Instrument> findBySubExchAndTotal(String subExch, int total);

	@Query(value= "{subExch:{$eq:?0},  type:{$eq:?1}}", sort="{ticker:1}")
	List<Instrument> findBySubExchAndType(String subExch, String type);

	@Query(value= "{subExch:{$eq:?0},  type:{$eq:?1}, ticker:{$gt:?2}}", sort="{ticker:1}")
	List<Instrument> findBySubExchAndTypeAndTicker(String subExch, String type, String ticker);

	List<Instrument> findByTicker(String ticker);

}