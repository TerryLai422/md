package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.Instrument;
import com.thinkbox.md.model.InstrumentCA;

import reactor.core.publisher.Flux;

public interface InstrumentCARepository extends ReactiveMongoRepository<InstrumentCA, String> {

	Flux<Instrument> findBySubExch(String subExch);
}