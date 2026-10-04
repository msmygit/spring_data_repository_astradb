package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import org.junit.jupiter.api.Test;

import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;

import static org.assertj.core.api.Assertions.assertThat;

class SpringEnvironmentDriverConfigSupplierTests {

	@Test
	void yamlOptionsReachTheDriver() throws Exception {
		StandardEnvironment environment = environmentWithYaml("""
				datastax-java-driver:
				  basic:
				    request:
				      timeout: 3 seconds
				      consistency: LOCAL_ONE
				  advanced:
				    connection:
				      pool:
				        local:
				          size: 2
				    metadata:
				      schema:
				        refreshed-keyspaces:
				          - ks1
				          - ks2
				  profiles:
				    slow:
				      basic.request.timeout: 30 seconds
				""");
		DriverExecutionProfile profile = defaultProfile(environment);

		assertThat(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT)).isEqualTo(Duration.ofSeconds(3));
		assertThat(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("LOCAL_ONE");
		assertThat(profile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).isEqualTo(2);
		assertThat(profile.getStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES))
			.containsExactly("ks1", "ks2");
		// untouched options keep the driver's reference.conf defaults
		assertThat(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(5000);
		// execution profiles inherit from the default profile
		DriverExecutionProfile slow = loader(environment).getInitialConfig().getProfile("slow");
		assertThat(slow.getDuration(DefaultDriverOption.REQUEST_TIMEOUT)).isEqualTo(Duration.ofSeconds(30));
		assertThat(slow.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("LOCAL_ONE");
	}

	@Test
	void higherPrecedenceSourcesWinAndChangesAreSeenOnEveryCall() throws Exception {
		StandardEnvironment environment = environmentWithYaml("""
				datastax-java-driver.basic.request.timeout: 3 seconds
				""");
		SpringEnvironmentDriverConfigSupplier supplier = new SpringEnvironmentDriverConfigSupplier(environment);
		assertThat(supplier.get().getDuration("basic.request.timeout"))
			.isEqualTo(Duration.ofSeconds(3));

		environment.getPropertySources()
			.addFirst(new MapPropertySource("override", Map.of("datastax-java-driver.basic.request.timeout", "7s")));

		assertThat(supplier.get().getDuration("basic.request.timeout"))
			.isEqualTo(Duration.ofSeconds(7));
	}

	private static DriverExecutionProfile defaultProfile(StandardEnvironment environment) {
		return loader(environment).getInitialConfig().getDefaultProfile();
	}

	private static DefaultDriverConfigLoader loader(StandardEnvironment environment) {
		return new DefaultDriverConfigLoader(new SpringEnvironmentDriverConfigSupplier(environment), true);
	}

	private static StandardEnvironment environmentWithYaml(String yaml) throws Exception {
		StandardEnvironment environment = new StandardEnvironment();
		List<PropertySource<?>> sources = new YamlPropertySourceLoader()
			.load("test.yaml", new ByteArrayResource(yaml.getBytes()));
		sources.forEach(environment.getPropertySources()::addLast);
		return environment;
	}

}
