package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.Historical;

public interface HistoricalRepository extends MongoRepository<Historical, String> {


}