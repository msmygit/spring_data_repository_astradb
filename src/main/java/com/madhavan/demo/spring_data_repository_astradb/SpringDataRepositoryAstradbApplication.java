package com.madhavan.demo.spring_data_repository_astradb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class SpringDataRepositoryAstradbApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(SpringDataRepositoryAstradbApplication.class,
				args);
		// The demo is a one-shot run: shut down cleanly once it is done, unless asked to stay up
		// (--spring.main.keep-alive=true), e.g. to try driver configuration hot-reload.
		if (!context.getEnvironment().getProperty("spring.main.keep-alive", Boolean.class, false)) {
			System.exit(SpringApplication.exit(context));
		}
	}

}
