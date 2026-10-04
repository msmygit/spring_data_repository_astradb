package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.nio.file.Files;
import java.nio.file.Path;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;

import org.springframework.boot.cassandra.autoconfigure.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Wires the Spring Boot auto-configured {@code CqlSession} to Astra DB.
 * <ul>
 * <li>Connection details (secure connect bundle + token) are applied through a {@link CqlSessionBuilderCustomizer}, the
 * extension point Spring Boot offers for exactly this purpose.</li>
 * <li>Driver options come from the {@code datastax-java-driver.*} section of {@code application.yaml} (or any other
 * Spring property source) via a reloadable {@link DriverConfigLoader}. See {@link SpringEnvironmentDriverConfigSupplier}
 * and {@link DriverConfigReloader}.</li>
 * <li>{@code spring.cassandra.schema-action} is applied with Astra-compatible SAI DDL. See
 * {@link AstraSchemaCreator}.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class AstraCassandraConfiguration {

	/**
	 * Astra uses token authentication: the literal user name {@code "token"} and the {@code AstraCS:...} token as the
	 * password. The bundle carries contact points, the local datacenter and the mTLS material, so none of those are
	 * configured anywhere else.
	 */
	@Bean
	CqlSessionBuilderCustomizer astraSessionBuilderCustomizer(AstraDbProperties astra) {
		Assert.state(StringUtils.hasText(astra.applicationToken()),
				"ASTRA_DB_APPLICATION_TOKEN is not set (property astra.db.application-token)");
		Path bundle = astra.secureConnectBundle();
		Assert.state(bundle != null && !bundle.toString().isBlank(),
				"ASTRA_DB_SECURE_BUNDLE_PATH is not set (property astra.db.secure-connect-bundle)");
		Assert.state(Files.isReadable(bundle), () -> "Secure connect bundle not found or not readable: " + bundle);
		return builder -> builder.withCloudSecureConnectBundle(bundle)
			.withAuthCredentials("token", astra.applicationToken());
	}

	/**
	 * Replaces Spring Boot's default loader (which only maps a fixed subset of {@code spring.cassandra.*} and always adds
	 * a {@code 127.0.0.1:9042} contact point) with one that:
	 * <ul>
	 * <li>accepts <em>any</em> option from the driver's
	 * <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/">reference
	 * configuration</a> under {@code datastax-java-driver:} in YAML, and</li>
	 * <li>supports reloading: every {@link DriverConfigLoader#reload()} re-reads the Spring {@code Environment}.</li>
	 * </ul>
	 * The session closes the loader, hence the empty destroy method (same as Spring Boot's own bean).
	 */
	@Bean(destroyMethod = "")
	DriverConfigLoader cassandraDriverConfigLoader(ConfigurableEnvironment environment) {
		return new DefaultDriverConfigLoader(new SpringEnvironmentDriverConfigSupplier(environment), true);
	}

	/**
	 * Same as Spring Boot's auto-configured session factory (which backs off when this bean is present), but creates
	 * {@code @SaiIndexed} indexes with {@code CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'}.
	 */
	@Bean
	SessionFactoryFactoryBean cassandraSessionFactory(CqlSession session, CassandraConverter converter,
			ConfigurableEnvironment environment) {
		AstraSessionFactoryFactoryBean sessionFactory = new AstraSessionFactoryFactoryBean();
		sessionFactory.setSession(session);
		sessionFactory.setConverter(converter);
		Binder.get(environment)
			.bind("spring.cassandra.schema-action", SchemaAction.class)
			.ifBound(sessionFactory::setSchemaAction);
		return sessionFactory;
	}

}
