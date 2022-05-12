package com.thinkbox.md.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;

import com.mongodb.DBObject;
import com.mongodb.client.result.UpdateResult;
import com.thinkbox.md.model.Analysis;
import com.thinkbox.md.model.AnalysisETF;

public class AnalysisETFRepositoryImpl implements AnalysisRepositoryCustom {

	@Autowired
	MongoTemplate mongoTemplate;

	@Override
	public long updateField(String ticker, String date, String name, Object value) {

		Query query = new Query(Criteria.where("ticker").is(ticker).andOperator(Criteria.where("date").is(date)));
		Update update = new Update();
		if (value == null) {
			update.unset(name);

		} else {
			update.set(name, value);
		}

		UpdateResult result = mongoTemplate.updateFirst(query, update, AnalysisETF.class);

		if (result != null)
			return result.getModifiedCount();
		else
			return 0l;
	}
	
//	@Query(value="{_id: { $regex: '.*@20220504-000000' }, $where: ?0}", fields= "{ _id: 1}")

	public int countByCriterion(String date, String criterion) {
//		Query query = new Query(Criteria.where("id").regex(".*@20220504-000000"));
//	       
//		return 0;
//		GroupOperation group = Aggregation.group("date");
//		AggregationExpression aggregationExpression = AggregationExpression.from(MongoExpression.create("$expr: { $gt: ['$ind.sma50','$ind.sma200'] }"));
		AggregationExpression aggregationExpression = AggregationExpression
				.from(MongoExpression.create(" date: '20220504', $expr: { $gt: ['ind.sma50','ind.sma200'] }"));

		MatchOperation match = Aggregation.match(aggregationExpression);

		Aggregation aggregation = Aggregation.newAggregation(match, Aggregation.group().count().as("Total"));
		AggregationResults<DBObject> results = mongoTemplate.aggregate(aggregation,
				mongoTemplate.getCollectionName(AnalysisETF.class), DBObject.class);
		System.out.println(results.getMappedResults().toString());
		return 0;
	}

}
