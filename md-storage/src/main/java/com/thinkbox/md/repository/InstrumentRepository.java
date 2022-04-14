package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.Historical;
import com.thinkbox.md.model.Instrument;

public interface InstrumentRepository extends MongoRepository<Instrument, String> {


}