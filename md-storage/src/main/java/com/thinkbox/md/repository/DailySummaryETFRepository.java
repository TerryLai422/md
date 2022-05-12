package com.thinkbox.md.repository;


import org.springframework.data.mongodb.repository.MongoRepository;

import com.thinkbox.md.model.DailySummaryETF;

public interface DailySummaryETFRepository extends MongoRepository<DailySummaryETF, String> {

}