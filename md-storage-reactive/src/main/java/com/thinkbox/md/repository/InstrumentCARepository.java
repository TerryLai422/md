package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.InstrumentCA;

public interface InstrumentCARepository extends ReactiveMongoRepository<InstrumentCA, String> {

	List<InstrumentCA> findBySubExch(String subExch);

}