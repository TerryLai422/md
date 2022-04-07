package com.thinkbox.md;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

@EnableEurekaClient
@SpringBootApplication
public class MDApiGatewayApplication {

	public static void main(String[] args) {
		SpringApplication.run(MDApiGatewayApplication.class, args);
	}

}
