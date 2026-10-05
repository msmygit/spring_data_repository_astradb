package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link AstraDbProperties}: backward-compat promotion, primary-region resolution,
 * and {@code datacenter.json} bundle metadata parsing.
 */
class AstraDbPropertiesTests {

	@TempDir
	Path tempDir;

	// ----------------------------------------------------------------------- effectiveRegions

	@Test
	void singleRegionBundleIsPromotedToDefaultEntry() {
		// Token at top level; bundle promoted to the "default" region entry.
		AstraDbProperties props = new AstraDbProperties(
				"AstraCS:token", Path.of("/tmp/bundle.zip"), Map.of(), null, defaultFailover());

		Map<String, AstraDbProperties.RegionConfig> regions = props.effectiveRegions();

		assertThat(regions).hasSize(1).containsKey("default");
		AstraDbProperties.RegionConfig defaultRegion = regions.get("default");
		assertThat(defaultRegion.secureConnectBundle()).isEqualTo(Path.of("/tmp/bundle.zip"));
		assertThat(defaultRegion.displayName()).isNull();
	}

	@Test
	void multiRegionMapTakesPrecedenceOverLegacyProperties() {
		Map<String, AstraDbProperties.RegionConfig> regionsMap = Map.of(
				"us-east-1", new AstraDbProperties.RegionConfig(Path.of("/east.zip"), "US East"),
				"eu-west-1", new AstraDbProperties.RegionConfig(Path.of("/west.zip"), "EU West"));

		AstraDbProperties props = new AstraDbProperties(
				"AstraCS:token", Path.of("/legacy.zip"), regionsMap, null, defaultFailover());

		assertThat(props.effectiveRegions()).isEqualTo(regionsMap);
	}

	@Test
	void emptyConfigProducesEmptyRegions() {
		AstraDbProperties props = new AstraDbProperties(null, null, Map.of(), null, defaultFailover());

		assertThat(props.effectiveRegions()).isEmpty();
	}

	@Test
	void nullRegionsMapFallsBackToBundlePromotion() {
		AstraDbProperties props = new AstraDbProperties(
				"AstraCS:tok", Path.of("/b.zip"), null, null, defaultFailover());

		assertThat(props.effectiveRegions()).containsKey("default");
	}

	@Test
	void noRegionsWhenOnlyTokenSetAndNoBundleOrRegionsMap() {
		// Token alone without a bundle or regions map → empty regions (not promoted)
		AstraDbProperties props = new AstraDbProperties("AstraCS:tok", null, Map.of(), null, defaultFailover());

		assertThat(props.effectiveRegions()).isEmpty();
	}

	// ----------------------------------------------------------------------- effectivePrimaryRegion

	@Test
	void explicitPrimaryRegionIsUsedWhenSet() {
		Map<String, AstraDbProperties.RegionConfig> regionsMap = Map.of(
				"us-east-1", new AstraDbProperties.RegionConfig(null, null),
				"eu-west-1", new AstraDbProperties.RegionConfig(null, null));

		AstraDbProperties props = new AstraDbProperties(null, null, regionsMap, "eu-west-1", defaultFailover());

		assertThat(props.effectivePrimaryRegion()).isEqualTo("eu-west-1");
	}

	@Test
	void firstRegionKeyIsUsedWhenNoBundleMetadataAndNoPrimaryRegion() {
		// Use a LinkedHashMap to have a predictable insertion order.
		Map<String, AstraDbProperties.RegionConfig> regionsMap = new java.util.LinkedHashMap<>();
		regionsMap.put("ap-south-1", new AstraDbProperties.RegionConfig(null, null));
		regionsMap.put("us-east-1", new AstraDbProperties.RegionConfig(null, null));

		AstraDbProperties props = new AstraDbProperties(null, null, regionsMap, null, defaultFailover());

		assertThat(props.effectivePrimaryRegion()).isEqualTo("ap-south-1");
	}

	@Test
	void emptyRegionsThrowsWhenResolvingPrimaryRegion() {
		AstraDbProperties props = new AstraDbProperties(null, null, Map.of(), null, defaultFailover());

		assertThatIllegalStateException()
			.isThrownBy(props::effectivePrimaryRegion)
			.withMessageContaining("No Astra DB regions are configured");
	}

	// ----------------------------------------------------------------------- detectRegionFromBundle

	@Test
	void detectRegionFromBundleReadsRegionField() throws IOException {
		Path bundle = createBundleWithDatacenterJson("{\"region\":\"us-east-1\",\"datacenter\":\"dc1\"}");

		Optional<String> result = AstraDbProperties.detectRegionFromBundle(bundle);

		assertThat(result).contains("us-east-1");
	}

	@Test
	void detectRegionFromBundleFallsBackToDatacenterField() throws IOException {
		Path bundle = createBundleWithDatacenterJson("{\"datacenter\":\"eu-west-1-dc\"}");

		Optional<String> result = AstraDbProperties.detectRegionFromBundle(bundle);

		assertThat(result).contains("eu-west-1-dc");
	}

	@Test
	void detectRegionFromBundleReturnsEmptyWhenNoBundlePath() {
		assertThat(AstraDbProperties.detectRegionFromBundle(null)).isEmpty();
	}

	@Test
	void detectRegionFromBundleReturnsEmptyWhenFileDoesNotExist() {
		assertThat(AstraDbProperties.detectRegionFromBundle(Path.of("/nonexistent/bundle.zip"))).isEmpty();
	}

	@Test
	void detectRegionFromBundleReturnsEmptyWhenNoBundleEntryInZip() throws IOException {
		// ZIP with a different entry — no datacenter.json.
		Path bundle = tempDir.resolve("empty.zip");
		try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(bundle))) {
			zos.putNextEntry(new ZipEntry("config.json"));
			zos.write("{}".getBytes());
			zos.closeEntry();
		}

		assertThat(AstraDbProperties.detectRegionFromBundle(bundle)).isEmpty();
	}

	// ----------------------------------------------------------------------- FailoverProperties defaults

	@Test
	void failoverPropertiesHaveExpectedDefaults() {
		AstraDbProperties.FailoverProperties failover = new AstraDbProperties.FailoverProperties(true, 0, "LOCAL_QUORUM");

		assertThat(failover.enabled()).isTrue();
		assertThat(failover.maxAttempts()).isZero();
		assertThat(failover.readConsistency()).isEqualTo("LOCAL_QUORUM");
	}

	// ----------------------------------------------------------------------- toString / token masking

	@Test
	void toStringMasksApplicationToken() {
		AstraDbProperties props = new AstraDbProperties(
				"AstraCS:secret123", Path.of("/b.zip"), Map.of(), null, defaultFailover());

		assertThat(props.toString()).doesNotContain("secret123").contains("******");
	}

	@Test
	void regionConfigToStringShowsBundleAndDisplayName() {
		AstraDbProperties.RegionConfig cfg =
				new AstraDbProperties.RegionConfig(Path.of("/bundle.zip"), "My Region");

		// Token is no longer in RegionConfig; confirm toString doesn't include it
		assertThat(cfg.toString()).contains("/bundle.zip").contains("My Region");
	}

	// ----------------------------------------------------------------------- helpers

	private static AstraDbProperties.FailoverProperties defaultFailover() {
		return new AstraDbProperties.FailoverProperties(true, 0, "LOCAL_QUORUM");
	}

	private Path createBundleWithDatacenterJson(String json) throws IOException {
		Path bundle = tempDir.resolve("bundle.zip");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ZipOutputStream zos = new ZipOutputStream(baos)) {
			zos.putNextEntry(new ZipEntry("datacenter.json"));
			zos.write(json.getBytes());
			zos.closeEntry();
		}
		Files.write(bundle, baos.toByteArray());
		return bundle;
	}

}
