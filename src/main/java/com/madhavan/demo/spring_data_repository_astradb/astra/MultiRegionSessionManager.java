package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Manages multiple {@link CqlSession} instances across configured Astra DB regions.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Eagerly creates and warms a {@link CqlSession} per region at startup via {@link #init()}.</li>
 *   <li>Designates an active primary region (auto-detected or configured) and tracks healthy standby regions.</li>
 *   <li>Provides fast failover switching between regions when failures occur.</li>
 *   <li>Configures standby sessions with minimal connection pool settings to save resources.</li>
 *   <li>Executes lightweight periodic or on-demand health checks against {@code system.local}.</li>
 *   <li>Publishes Micrometer metrics for session health, active region, failover counts, and failover latency.</li>
 *   <li>Gracefully closes all open sessions during container shutdown via {@link #close()}.</li>
 * </ul>
 */
public class MultiRegionSessionManager implements Closeable {

	public static final String STANDBY_PROFILE = "standby";
	private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(5);
	private static final SimpleStatement HEALTH_CHECK_STMT =
			SimpleStatement.newInstance("SELECT now() FROM system.local").setTimeout(HEALTH_CHECK_TIMEOUT);

	private static final Logger log = LoggerFactory.getLogger(MultiRegionSessionManager.class);

	private final AstraDbProperties properties;
	private final ConfigurableEnvironment environment;
	private final MeterRegistry meterRegistry;
	private final Function<RegionSessionContext, CqlSession> sessionFactory;

	private final Map<String, CqlSession> sessions = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> healthStatus = new ConcurrentHashMap<>();
	private final AtomicReference<String> activeRegion = new AtomicReference<>();
	private final List<String> configuredRegions = new ArrayList<>();

	private Counter failoverCounter;
	private Timer failoverTimer;

	public MultiRegionSessionManager(AstraDbProperties properties,
			ConfigurableEnvironment environment,
			ObjectProvider<MeterRegistry> meterRegistryProvider) {
		this(properties, environment, meterRegistryProvider.getIfAvailable(), null);
	}

	public MultiRegionSessionManager(AstraDbProperties properties,
			ConfigurableEnvironment environment,
			MeterRegistry meterRegistry,
			Function<RegionSessionContext, CqlSession> sessionFactory) {
		this.properties = Objects.requireNonNull(properties, "AstraDbProperties must not be null");
		this.environment = Objects.requireNonNull(environment, "ConfigurableEnvironment must not be null");
		this.meterRegistry = meterRegistry;
		this.sessionFactory = sessionFactory != null ? sessionFactory : this::defaultCreateSession;
	}

	/**
	 * Context passed to session factory functions.
	 */
	public record RegionSessionContext(String regionName, AstraDbProperties.RegionConfig config,
			DriverConfigLoader configLoader, boolean isPrimary) {
	}

	@PostConstruct
	public void init() {
		Map<String, AstraDbProperties.RegionConfig> regions = this.properties.effectiveRegions();
		Assert.state(!regions.isEmpty(), "No Astra DB regions are configured");

		String primary = this.properties.effectivePrimaryRegion();
		Assert.state(regions.containsKey(primary),
				() -> "Primary region '" + primary + "' is not present in configured regions: " + regions.keySet());

		this.activeRegion.set(primary);
		this.configuredRegions.clear();
		// Place primary first, followed by remaining regions
		this.configuredRegions.add(primary);
		regions.keySet().stream()
				.filter(r -> !r.equals(primary))
				.forEach(this.configuredRegions::add);

		initMetrics();

		log.info("Initializing Astra DB MultiRegionSessionManager for regions: {} (primary: {})",
				this.configuredRegions, primary);

		for (Map.Entry<String, AstraDbProperties.RegionConfig> entry : regions.entrySet()) {
			String region = entry.getKey();
			AstraDbProperties.RegionConfig config = entry.getValue();
			boolean isPrimary = region.equals(primary);

			try {
				CqlSession session = createSessionForRegion(region, config, isPrimary);
				this.sessions.put(region, session);
				boolean healthy = checkSessionHealth(session);
				setHealthStatus(region, healthy);
				log.info("Initialized Astra DB session for region '{}' (primary: {}, healthy: {})",
						region, isPrimary, healthy);
			}
			catch (Exception ex) {
				setHealthStatus(region, false);
				log.error("Failed to initialize Astra DB session for region '{}'", region, ex);
				if (isPrimary && regions.size() == 1) {
					throw new IllegalStateException("Failed to initialize session for sole region '" + region + "'", ex);
				}
			}
		}

		// If initial active region is unhealthy, attempt failover to first healthy region
		if (!isHealthy(this.activeRegion.get())) {
			List<String> available = getAvailableRegions();
			if (!available.isEmpty()) {
				String fallback = available.get(0);
				log.warn("Primary region '{}' is unhealthy at startup. Failing over to '{}'",
						this.activeRegion.get(), fallback);
				this.activeRegion.set(fallback);
			}
			else {
				log.warn("All Astra DB sessions are currently reporting unhealthy");
			}
		}
	}

	private void initMetrics() {
		if (this.meterRegistry == null) {
			return;
		}

		this.failoverCounter = Counter.builder("astradb.failover.total")
				.description("Total number of cross-region failover events")
				.register(this.meterRegistry);

		this.failoverTimer = Timer.builder("astradb.failover.latency")
				.description("Latency of cross-region failover operations")
				.register(this.meterRegistry);

		this.meterRegistry.gauge("astradb.failover.active_region", Tags.empty(), this,
				mgr -> {
					String current = mgr.getActiveRegion();
					int idx = mgr.configuredRegions.indexOf(current);
					return idx >= 0 ? idx : -1;
				});

		for (String region : this.configuredRegions) {
			AtomicInteger healthGauge = this.healthStatus.computeIfAbsent(region, r -> new AtomicInteger(0));
			this.meterRegistry.gauge("astradb.session.healthy", Tags.of("region", region), healthGauge, AtomicInteger::get);
		}
	}

	private CqlSession createSessionForRegion(String region, AstraDbProperties.RegionConfig config, boolean isPrimary) {
		Supplier<com.typesafe.config.Config> baseSupplier = new SpringEnvironmentDriverConfigSupplier(this.environment);
		Supplier<com.typesafe.config.Config> supplier = isPrimary
				? baseSupplier
				: () -> com.typesafe.config.ConfigFactory.parseMap(Map.of(
								"profiles." + STANDBY_PROFILE + ".basic.request.consistency", "LOCAL_QUORUM",
								"advanced.connection.pool.remote.size", 1))
						.withFallback(baseSupplier.get());

		DriverConfigLoader configLoader = new DefaultDriverConfigLoader(supplier, true);
		return this.sessionFactory.apply(new RegionSessionContext(region, config, configLoader, isPrimary));
	}

	private CqlSession defaultCreateSession(RegionSessionContext ctx) {
		// The application token is shared across all regions and lives at the top level.
		String token = this.properties.applicationToken();
		Assert.state(StringUtils.hasText(token),
				"ASTRA_DB_APPLICATION_TOKEN is not set (property astra.db.application-token)");
		Path bundle = ctx.config().secureConnectBundle();
		Assert.state(bundle != null && !bundle.toString().isBlank(),
				() -> "Secure connect bundle is not set for region '" + ctx.regionName() + "'");
		Assert.state(Files.isReadable(bundle),
				() -> "Secure connect bundle not found or not readable for region '" + ctx.regionName() + "': " + bundle);

		return CqlSession.builder()
				.withCloudSecureConnectBundle(bundle)
				.withAuthCredentials("token", token)
				.withConfigLoader(ctx.configLoader())
				.build();
	}

	/**
	 * Returns the {@link CqlSession} for the currently active region.
	 */
	public CqlSession getActiveSession() {
		String active = this.activeRegion.get();
		CqlSession session = active != null ? this.sessions.get(active) : null;
		if (session == null) {
			throw new IllegalStateException("No active Astra DB session available (active region: '" + active + "')");
		}
		return session;
	}

	/**
	 * Returns the name of the currently active region.
	 */
	public String getActiveRegion() {
		return this.activeRegion.get();
	}

	/**
	 * Returns an unmodifiable map of all managed region names to sessions.
	 */
	public Map<String, CqlSession> getSessions() {
		return Collections.unmodifiableMap(this.sessions);
	}

	/**
	 * Returns the {@link CqlSession} for a specific region if present.
	 */
	public CqlSession getSession(String region) {
		return this.sessions.get(region);
	}

	/**
	 * Switches the active region to the target region if healthy.
	 *
	 * @param targetRegion region name to failover to
	 * @throws IllegalArgumentException if region is not configured or not healthy
	 */
	public void failoverTo(String targetRegion) {
		Assert.hasText(targetRegion, "Target region must not be empty");
		Assert.isTrue(this.sessions.containsKey(targetRegion),
				() -> "Unknown target region '" + targetRegion + "'. Configured regions: " + this.sessions.keySet());

		String current = this.activeRegion.get();
		if (targetRegion.equals(current)) {
			log.info("Failover target region is already the active region ('{}')", targetRegion);
			return;
		}

		Runnable failoverAction = () -> {
			if (!isHealthy(targetRegion)) {
				throw new IllegalStateException("Cannot failover to region '" + targetRegion + "': region is not healthy");
			}

			this.activeRegion.set(targetRegion);
			if (this.failoverCounter != null) {
				this.failoverCounter.increment();
			}
			log.warn("Failover completed: active Astra DB region switched from '{}' to '{}'", current, targetRegion);
		};

		if (this.failoverTimer != null) {
			this.failoverTimer.record(failoverAction);
		}
		else {
			failoverAction.run();
		}
	}

	/**
	 * Returns the list of configured regions in priority order.
	 */
	public List<String> getConfiguredRegions() {
		return Collections.unmodifiableList(this.configuredRegions);
	}

	/**
	 * Returns the list of healthy regions currently available.
	 */
	public List<String> getAvailableRegions() {
		List<String> available = new ArrayList<>();
		for (String region : this.configuredRegions) {
			if (isHealthy(region)) {
				available.add(region);
			}
		}
		return available;
	}

	/**
	 * Checks if a specific region is currently marked healthy.
	 */
	public boolean isHealthy(String region) {
		AtomicInteger status = this.healthStatus.get(region);
		return status != null && status.get() == 1;
	}

	/**
	 * Executes an active probe on the specified region's session and updates health status.
	 */
	public boolean probeHealth(String region) {
		CqlSession session = this.sessions.get(region);
		if (session == null || session.isClosed()) {
			setHealthStatus(region, false);
			return false;
		}
		boolean healthy = checkSessionHealth(session);
		setHealthStatus(region, healthy);
		return healthy;
	}

	/**
	 * Executes an active probe on all sessions and returns a snapshot of health statuses.
	 */
	public Map<String, Boolean> probeAll() {
		Map<String, Boolean> snapshot = new LinkedHashMap<>();
		for (String region : this.configuredRegions) {
			snapshot.put(region, probeHealth(region));
		}
		return snapshot;
	}

	private boolean checkSessionHealth(CqlSession session) {
		try {
			session.execute(HEALTH_CHECK_STMT);
			return true;
		}
		catch (Exception ex) {
			log.debug("Session health check failed: {}", ex.getMessage());
			return false;
		}
	}

	public void setHealthStatus(String region, boolean healthy) {
		this.healthStatus.computeIfAbsent(region, r -> new AtomicInteger(0))
				.set(healthy ? 1 : 0);
	}

	@PreDestroy
	@Override
	public void close() {
		log.info("Closing all Astra DB sessions in MultiRegionSessionManager...");
		for (Map.Entry<String, CqlSession> entry : this.sessions.entrySet()) {
			String region = entry.getKey();
			CqlSession session = entry.getValue();
			if (session != null && !session.isClosed()) {
				try {
					session.close();
					log.info("Closed Astra DB session for region '{}'", region);
				}
				catch (Exception ex) {
					log.warn("Error closing Astra DB session for region '{}'", region, ex);
				}
			}
		}
		this.sessions.clear();
	}

	@Override
	public String toString() {
		return "MultiRegionSessionManager[activeRegion=" + this.activeRegion.get()
				+ ", regions=" + this.configuredRegions
				+ ", health=" + this.healthStatus
				+ "]";
	}

}
