package com.thinkbox.md.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import com.thinkbox.md.model.DailySummary;
import com.thinkbox.md.model.TradeDate;

public interface DailySummaryRepository extends MongoRepository<DailySummary, String> {

}