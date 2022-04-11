package com.thinkbox.md.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class CSVFileReader {

	private static final Logger logger = LoggerFactory.getLogger(CSVFileReader.class);

	public List<String[]> read(String fileName, Character separator, Character quote) throws IOException {

		List<String[]> list = new ArrayList<>();

		FileReader fileReader = new FileReader(fileName);
		Reader reader = new BufferedReader(fileReader);

		CSVParserBuilder builder = new CSVParserBuilder();
		if (separator != null) {
			builder.withSeparator(separator);
		}
		if (quote != null) {
			builder.withQuoteChar(quote);
		}
		CSVParser parser = builder.build();
		CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).withCSVParser(parser).build();

		list = csvReader.readAll();
		reader.close();
		csvReader.close();
		return list;
	}

}