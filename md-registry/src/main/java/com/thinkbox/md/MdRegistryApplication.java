package com.thinkbox.md;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@EnableEurekaServer
@SpringBootApplication
public class MdRegistryApplication {

	public static void main(String[] args) {
		SpringApplication.run(MdRegistryApplication.class, args);
	}

}
