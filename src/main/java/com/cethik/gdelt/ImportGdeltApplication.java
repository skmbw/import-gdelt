package com.cethik.gdelt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.cethik.gdelt")
public class ImportGdeltApplication {

	public static void main(String[] args) {
		SpringApplication.run(ImportGdeltApplication.class, args);
	}
}
