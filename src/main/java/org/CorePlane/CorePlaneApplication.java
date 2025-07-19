package org.CorePlane;

import org.CorePlane.configurations.LicenseCheck;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CorePlaneApplication {

	public final LicenseCheck licenseCheck;

	public CorePlaneApplication(LicenseCheck licenseCheck) {
		this.licenseCheck = licenseCheck;
	}

	public static void main(String[] args) {

		var app = new SpringApplication(CorePlaneApplication.class);
		var context = app.run(args);

		LicenseCheck licenseCheck = context.getBean(LicenseCheck.class);

		String licenseKey = System.getenv("LICENSE_KEY");
		if (licenseKey == null || !licenseCheck.checkLicense(licenseKey)) {
			System.err.println("ERROR: Invalid or missing license key. Application will exit.");
			System.exit(1);
		}

		System.out.println("License validated successfully. Starting application...");
	}
}