package com.thinkbox.md;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

import reactor.blockhound.BlockHound;

//@EnableEurekaClient
@SpringBootApplication
public class MDFileParserApplication {

	static {
		BlockHound.install();
	}

	public static void main(String[] args) {
		SpringApplication.run(MDFileParserApplication.class, args);
	}

}
