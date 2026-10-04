package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Astra DB connection settings. Values are bound from environment variables in {@code application.yaml}; nothing secret
 * is stored in the repository.
 *
 * @param applicationToken the Astra application token ({@code AstraCS:...}), from {@code ASTRA_DB_APPLICATION_TOKEN}
 * @param secureConnectBundle absolute path to the secure connect bundle zip, from {@code ASTRA_DB_SECURE_BUNDLE_PATH}
 */
@ConfigurationProperties("astra.db")
public record AstraDbProperties(String applicationToken, Path secureConnectBundle) {

	@Override
	public String toString() {
		// Never leak the token into logs or actuator output.
		return "AstraDbProperties[applicationToken=" + (hasText(applicationToken) ? "******" : "<unset>")
				+ ", secureConnectBundle=" + secureConnectBundle + "]";
	}

	private static boolean hasText(String value) {
		return value != null && !value.isBlank();
	}

}
