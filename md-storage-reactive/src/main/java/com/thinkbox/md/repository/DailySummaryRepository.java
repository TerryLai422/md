package com.thinkbox.md.repository;


import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.DailySummary;

public interface DailySummaryRepository extends ReactiveMongoRepository<DailySummary, String> {

}