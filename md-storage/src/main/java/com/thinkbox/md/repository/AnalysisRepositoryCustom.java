package com.thinkbox.md.repository;

public interface AnalysisRepositoryCustom {

	public long updateField(String ticker, String date, String name, Object value);
	
	public int countByCriterion(String date, String criterion) ;
}
