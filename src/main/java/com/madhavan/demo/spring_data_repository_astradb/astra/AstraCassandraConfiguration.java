package com.madhavan.demo.spring_data_repository_astradb.astra;

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Spring configuration for multi-region Astra DB connectivity.
 *
 * <p>Replaces the single-session auto-configuration with three collaborating beans:
 * <ol>
 *   <li>{@link MultiRegionSessionManager} — owns all {@link CqlSession} instances (one per
 *       region), tracks the active region, and performs health checks.</li>
 *   <li>{@link FailoverCassandraOperations} — implements {@link CassandraOperations} by
 *       delegating to the active region's session and retrying on the next healthy region
 *       when a failover-eligible exception is thrown.</li>
 *   <li>{@link SessionFactoryFactoryBean} — runs schema DDL ({@code CREATE TABLE IF NOT EXISTS}
 *       etc.) once at startup against the active (primary) session.</li>
 * </ol>
 *
 * <p>{@link DriverConfigReloader} is wired separately via its own {@link org.springframework.stereotype.Component}
 * annotation and continues to hot-reload config on file changes; it is updated here to reload
 * all sessions managed by {@link MultiRegionSessionManager}.
 */
@Configuration(proxyBeanMethods = false)
public class AstraCassandraConfiguration {

	/**
	 * Creates and manages all Astra DB {@link CqlSession} instances.
	 *
	 * <p>This bean replaces the single-session {@code CqlSessionBuilderCustomizer} and
	 * {@code cassandraDriverConfigLoader} beans from the previous single-region configuration.
	 * Spring Boot's auto-configured {@code CqlSession} is disabled because
	 * {@link MultiRegionSessionManager} is a {@link org.springframework.stereotype.Component}
	 * that builds its own sessions, and the {@code cassandraSessionFactory} bean below
	 * exposes the primary session directly.
	 *
	 * <p>Note: {@link MultiRegionSessionManager} is already annotated with
	 * {@code @Component}, so Spring Boot picks it up automatically; this bean method
	 * exists here only to make the wiring explicit and to satisfy the
	 * {@link SessionFactoryFactoryBean} constructor below.
	 */
	@Bean
	MultiRegionSessionManager multiRegionSessionManager(AstraDbProperties properties,
			ConfigurableEnvironment environment,
			ObjectProvider<MeterRegistry> meterRegistryProvider) {
		return new MultiRegionSessionManager(properties, environment, meterRegistryProvider);
	}

	/**
	 * Exposes the active region's {@link CqlSession} as the primary {@code CqlSession} bean
	 * so that Spring Data's auto-configured {@link SessionFactoryFactoryBean} and any other
	 * infrastructure that injects {@code CqlSession} receive the correct session.
	 *
	 * <p>This is a live delegate — the returned session changes whenever
	 * {@link MultiRegionSessionManager#failoverTo(String)} is called.
	 */
	@Bean
	@Primary
	CqlSession cassandraSession(MultiRegionSessionManager sessionManager) {
		return sessionManager.getActiveSession();
	}

	/**
	 * Exposes {@link FailoverCassandraOperations} as the primary {@link CassandraOperations}
	 * bean, replacing the auto-configured {@code CassandraTemplate}.
	 *
	 * <p>All Spring Data repositories and custom repository implementations receive this
	 * failover-aware wrapper instead of a bare {@code CassandraTemplate}.
	 */
	@Bean
	@Primary
	CassandraOperations cassandraOperations(MultiRegionSessionManager sessionManager,
			CassandraConverter converter,
			AstraDbProperties properties,
			ObjectProvider<MeterRegistry> meterRegistryProvider) {
		return new FailoverCassandraOperations(sessionManager, converter, properties,
				meterRegistryProvider.getIfAvailable());
	}

	/**
	 * Same as Spring Boot's auto-configured session factory (which backs off when this bean
	 * is present), but creates {@code @SaiIndexed} indexes with
	 * {@code CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'}.
	 *
	 * <p>Schema DDL runs once at startup against the active (primary) session only. Astra DB
	 * keyspaces are global; tables and indexes are replicated automatically.
	 */
	@Bean
	SessionFactoryFactoryBean cassandraSessionFactory(MultiRegionSessionManager sessionManager,
			CassandraConverter converter,
			ConfigurableEnvironment environment) {
		AstraSessionFactoryFactoryBean sessionFactory = new AstraSessionFactoryFactoryBean();
		sessionFactory.setSession(sessionManager.getActiveSession());
		sessionFactory.setConverter(converter);
		Binder.get(environment)
				.bind("spring.cassandra.schema-action", SchemaAction.class)
				.ifBound(sessionFactory::setSchemaAction);
		return sessionFactory;
	}

}
