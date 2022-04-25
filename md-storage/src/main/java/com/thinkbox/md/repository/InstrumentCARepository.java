package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.InstrumentCA;

public interface InstrumentCARepository extends MongoRepository<InstrumentCA, String> {

	List<InstrumentCA> findBySubExchange(String subExchange);

}