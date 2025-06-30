package org.CorePlane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CorePlaneApplication {

	public static void main(String[] args) {
		SpringApplication.run(CorePlaneApplication.class, args);
	}

}