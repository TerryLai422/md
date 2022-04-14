package com.thinkbox.md.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "instrument")
public class Instrument {

	@Id
	private String id;
	
	private String symbol;
	
	private String name;
	
	private String exchange;
}