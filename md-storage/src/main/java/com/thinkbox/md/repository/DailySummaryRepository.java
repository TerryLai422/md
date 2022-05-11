package com.thinkbox.md.repository;


import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.DailySummary;

public interface DailySummaryRepository extends MongoRepository<DailySummary, String> {

}