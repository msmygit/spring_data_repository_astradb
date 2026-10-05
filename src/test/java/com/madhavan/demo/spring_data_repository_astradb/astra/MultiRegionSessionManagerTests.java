package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.core.env.StandardEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MultiRegionSessionManagerTests {

	private StandardEnvironment environment;
	private MeterRegistry meterRegistry;

	@BeforeEach
	void setUp() {
		this.environment = new StandardEnvironment();
		this.meterRegistry = new SimpleMeterRegistry();
	}

	private AstraDbProperties createProperties(String primaryRegion, Map<String, AstraDbProperties.RegionConfig> regions) {
		return new AstraDbProperties("AstraCS:shared-token", null, regions, primaryRegion,
				new AstraDbProperties.FailoverProperties(true, 0, "LOCAL_QUORUM"));
	}

	private AstraDbProperties.RegionConfig createRegionConfig() {
		return new AstraDbProperties.RegionConfig(Path.of("/tmp/bundle.zip"), null);
	}

	@Nested
	@DisplayName("Initialization & Active Region Selection")
	class InitializationTests {

		@Test
		void initializesAllConfiguredRegionsAndSetsActiveToPrimary() {
			Map<String, AstraDbProperties.RegionConfig> regions = new LinkedHashMap<>();
			regions.put("us-east-1", createRegionConfig());
			regions.put("eu-west-1", createRegionConfig());
			regions.put("ap-south-1", createRegionConfig());

			AstraDbProperties properties = createProperties("us-east-1", regions);

			Map<String, CqlSession> mockSessions = new LinkedHashMap<>();
			List<MultiRegionSessionManager.RegionSessionContext> capturedContexts = new ArrayList<>();

			for (String r : regions.keySet()) {
				CqlSession session = mock(CqlSession.class);
				given(session.execute(any(Statement.class))).willReturn(mock(ResultSet.class));
				mockSessions.put(r, session);
			}

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> {
						capturedContexts.add(ctx);
						return mockSessions.get(ctx.regionName());
					}
			);

			manager.init();

			assertThat(manager.getActiveRegion()).isEqualTo("us-east-1");
			assertThat(manager.getActiveSession()).isSameAs(mockSessions.get("us-east-1"));
			assertThat(manager.getConfiguredRegions()).containsExactly("us-east-1", "eu-west-1", "ap-south-1");
			assertThat(manager.getSessions()).hasSize(3);

			// Verify context flags & standby remote pool configuration
			assertThat(capturedContexts).hasSize(3);
			MultiRegionSessionManager.RegionSessionContext primaryCtx = capturedContexts.stream()
					.filter(c -> c.regionName().equals("us-east-1"))
					.findFirst().orElseThrow();
			assertThat(primaryCtx.isPrimary()).isTrue();

			MultiRegionSessionManager.RegionSessionContext standbyCtx = capturedContexts.stream()
					.filter(c -> c.regionName().equals("eu-west-1"))
					.findFirst().orElseThrow();
			assertThat(standbyCtx.isPrimary()).isFalse();
			assertThat(standbyCtx.configLoader().getInitialConfig().getDefaultProfile().getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE))
					.isEqualTo(1);
		}

		@Test
		void fallsBackToNextHealthyRegionIfPrimaryIsUnhealthyAtStartup() {
			Map<String, AstraDbProperties.RegionConfig> regions = new LinkedHashMap<>();
			regions.put("us-east-1", createRegionConfig());
			regions.put("eu-west-1", createRegionConfig());

			AstraDbProperties properties = createProperties("us-east-1", regions);

			CqlSession unhealthyPrimary = mock(CqlSession.class);
			given(unhealthyPrimary.execute(any(Statement.class))).willThrow(new RuntimeException("Connection refused"));

			CqlSession healthyStandby = mock(CqlSession.class);
			given(healthyStandby.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> ctx.regionName().equals("us-east-1") ? unhealthyPrimary : healthyStandby
			);

			manager.init();

			assertThat(manager.getActiveRegion()).isEqualTo("eu-west-1");
			assertThat(manager.getActiveSession()).isSameAs(healthyStandby);
			assertThat(manager.isHealthy("us-east-1")).isFalse();
			assertThat(manager.isHealthy("eu-west-1")).isTrue();
			assertThat(manager.getAvailableRegions()).containsExactly("eu-west-1");
		}

		@Test
		void throwsWhenSingleConfiguredRegionFailsInitialization() {
			Map<String, AstraDbProperties.RegionConfig> regions = Map.of("us-east-1", createRegionConfig());
			AstraDbProperties properties = createProperties("us-east-1", regions);

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> {
						throw new RuntimeException("Init error");
					}
			);

			assertThatIllegalStateException().isThrownBy(manager::init);
		}
	}

	@Nested
	@DisplayName("Failover Logic & Health Checks")
	class FailoverTests {

		private MultiRegionSessionManager manager;
		private CqlSession sessionUsEast;
		private CqlSession sessionEuWest;

		@BeforeEach
		void setUpSessions() {
			Map<String, AstraDbProperties.RegionConfig> regions = new LinkedHashMap<>();
			regions.put("us-east-1", createRegionConfig());
			regions.put("eu-west-1", createRegionConfig());

			AstraDbProperties properties = createProperties("us-east-1", regions);

			sessionUsEast = mock(CqlSession.class);
			given(sessionUsEast.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			sessionEuWest = mock(CqlSession.class);
			given(sessionEuWest.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> ctx.regionName().equals("us-east-1") ? sessionUsEast : sessionEuWest
			);
			manager.init();
		}

		@Test
		void failoverToSwitchesActiveRegionWhenTargetIsHealthy() {
			assertThat(manager.getActiveRegion()).isEqualTo("us-east-1");

			manager.failoverTo("eu-west-1");

			assertThat(manager.getActiveRegion()).isEqualTo("eu-west-1");
			assertThat(manager.getActiveSession()).isSameAs(sessionEuWest);

			// Metric checks
			assertThat(meterRegistry.get("astradb.failover.total").counter().count()).isEqualTo(1.0);
			assertThat(meterRegistry.get("astradb.failover.latency").timer().count()).isEqualTo(1L);
		}

		@Test
		void failoverToThrowsWhenTargetRegionIsUnhealthy() {
			manager.setHealthStatus("eu-west-1", false);

			assertThatIllegalStateException()
					.isThrownBy(() -> manager.failoverTo("eu-west-1"))
					.withMessageContaining("is not healthy");

			assertThat(manager.getActiveRegion()).isEqualTo("us-east-1");
		}

		@Test
		void failoverToThrowsOnUnknownRegion() {
			assertThatThrownBy(() -> manager.failoverTo("ap-south-1"))
					.isInstanceOf(IllegalArgumentException.class);
		}

		@Test
		void failoverToSameRegionIsNoOp() {
			manager.failoverTo("us-east-1");

			assertThat(manager.getActiveRegion()).isEqualTo("us-east-1");
			assertThat(meterRegistry.get("astradb.failover.total").counter().count()).isEqualTo(0.0);
		}

		@Test
		void probeHealthUpdatesSessionHealthStatus() {
			assertThat(manager.isHealthy("us-east-1")).isTrue();

			// Simulate failure on next query
			given(sessionUsEast.execute(any(Statement.class))).willThrow(new RuntimeException("Socket closed"));

			boolean healthy = manager.probeHealth("us-east-1");

			assertThat(healthy).isFalse();
			assertThat(manager.isHealthy("us-east-1")).isFalse();
			assertThat(manager.getAvailableRegions()).containsExactly("eu-west-1");
		}

		@Test
		void probeAllChecksEveryConfiguredRegion() {
			given(sessionUsEast.execute(any(Statement.class))).willReturn(mock(ResultSet.class));
			given(sessionEuWest.execute(any(Statement.class))).willThrow(new RuntimeException("Timeout"));

			Map<String, Boolean> results = manager.probeAll();

			assertThat(results).containsEntry("us-east-1", true);
			assertThat(results).containsEntry("eu-west-1", false);
			assertThat(manager.isHealthy("us-east-1")).isTrue();
			assertThat(manager.isHealthy("eu-west-1")).isFalse();
		}
	}

	@Nested
	@DisplayName("Micrometer Metrics")
	class MetricsTests {

		@Test
		void verifiesGaugesForActiveRegionAndPerRegionHealth() {
			Map<String, AstraDbProperties.RegionConfig> regions = new LinkedHashMap<>();
			regions.put("us-east-1", createRegionConfig());
			regions.put("eu-west-1", createRegionConfig());

			AstraDbProperties properties = createProperties("us-east-1", regions);

			CqlSession s1 = mock(CqlSession.class);
			given(s1.execute(any(Statement.class))).willReturn(mock(ResultSet.class));
			CqlSession s2 = mock(CqlSession.class);
			given(s2.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> ctx.regionName().equals("us-east-1") ? s1 : s2
			);
			manager.init();

			// Active region gauge: index in configuredRegions (us-east-1 = 0, eu-west-1 = 1)
			Gauge activeRegionGauge = meterRegistry.get("astradb.failover.active_region").gauge();
			assertThat(activeRegionGauge.value()).isEqualTo(0.0);

			// Healthy gauges
			Gauge usEastHealth = meterRegistry.get("astradb.session.healthy").tag("region", "us-east-1").gauge();
			Gauge euWestHealth = meterRegistry.get("astradb.session.healthy").tag("region", "eu-west-1").gauge();
			assertThat(usEastHealth.value()).isEqualTo(1.0);
			assertThat(euWestHealth.value()).isEqualTo(1.0);

			// Switch active region and check gauge updates
			manager.failoverTo("eu-west-1");
			assertThat(activeRegionGauge.value()).isEqualTo(1.0);

			// Change health and check gauge
			manager.setHealthStatus("us-east-1", false);
			assertThat(usEastHealth.value()).isEqualTo(0.0);
		}
	}

	@Nested
	@DisplayName("Lifecycle & PreDestroy Cleanup")
	class LifecycleTests {

		@Test
		void closeClosesAllManagedSessions() {
			Map<String, AstraDbProperties.RegionConfig> regions = new LinkedHashMap<>();
			regions.put("us-east-1", createRegionConfig());
			regions.put("eu-west-1", createRegionConfig());

			AstraDbProperties properties = createProperties("us-east-1", regions);

			CqlSession s1 = mock(CqlSession.class);
			given(s1.isClosed()).willReturn(false);
			given(s1.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			CqlSession s2 = mock(CqlSession.class);
			given(s2.isClosed()).willReturn(false);
			given(s2.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> ctx.regionName().equals("us-east-1") ? s1 : s2
			);
			manager.init();

			manager.close();

			verify(s1, times(1)).close();
			verify(s2, times(1)).close();
			assertThat(manager.getSessions()).isEmpty();
		}

		@Test
		void toStringProvidesInformativeOutputWithoutSecrets() {
			Map<String, AstraDbProperties.RegionConfig> regions = Map.of("us-east-1", createRegionConfig());
			AstraDbProperties properties = createProperties("us-east-1", regions);

			CqlSession s1 = mock(CqlSession.class);
			given(s1.execute(any(Statement.class))).willReturn(mock(ResultSet.class));

			MultiRegionSessionManager manager = new MultiRegionSessionManager(
					properties,
					environment,
					meterRegistry,
					ctx -> s1
			);
			manager.init();

			String str = manager.toString();
			assertThat(str).contains("activeRegion=us-east-1");
			assertThat(str).contains("regions=[us-east-1]");
			assertThat(str).doesNotContain("secret");
			assertThat(str).doesNotContain("AstraCS");
		}
	}

}
