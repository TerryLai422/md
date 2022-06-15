package com.thinkbox.md.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.thinkbox.md.model.DailySummaryETF;

public interface DailySummaryETFRepository extends ReactiveMongoRepository<DailySummaryETF, String> {

}