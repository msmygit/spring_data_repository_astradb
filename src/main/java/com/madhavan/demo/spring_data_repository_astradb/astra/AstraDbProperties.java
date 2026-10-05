package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.jspecify.annotations.Nullable;

/**
 * Astra DB connection settings for single-region and multi-region deployments.
 *
 * <p>The {@code applicationToken} ({@code AstraCS:...}) is a <em>global</em> credential shared
 * across all Astra DB regions of the same database — it is set once at the top level and is
 * <strong>not</strong> repeated per region.
 *
 * <p><b>Single-region (backward-compatible)</b>: set {@code astra.db.application-token} and
 * {@code astra.db.secure-connect-bundle} as before; the bundle is silently promoted to the
 * {@code "default"} region entry so the rest of the application can always iterate over
 * {@link #regions()}.
 *
 * <p><b>Multi-region</b>: set the shared token once, then populate {@code astra.db.regions}
 * with one entry per region (each entry only needs the region-specific bundle):
 * <pre>{@code
 * astra:
 *   db:
 *     application-token: ${ASTRA_DB_APPLICATION_TOKEN:}   # shared across all regions
 *     primary-region: us-east-1          # optional; auto-detected from bundle if absent
 *     regions:
 *       us-east-1:
 *         secure-connect-bundle: ${ASTRA_DB_BUNDLE_USEAST1:}
 *       eu-west-1:
 *         secure-connect-bundle: ${ASTRA_DB_BUNDLE_EUWEST1:}
 *   failover:
 *     enabled: true
 *     max-attempts: 1          # default: number-of-regions - 1
 *     read-consistency: LOCAL_QUORUM
 * }</pre>
 *
 * @param applicationToken    Astra DB token ({@code AstraCS:...}); shared by all regions
 * @param secureConnectBundle single-region bundle path; use {@code regions} for multi-region
 * @param regions             map of region-name → {@link RegionConfig}; populated from {@code astra.db.regions.*}
 * @param primaryRegion       explicit primary region key; {@code null} to auto-detect from bundle metadata
 * @param failover            failover policy settings
 */
@ConfigurationProperties("astra.db")
public record AstraDbProperties(
		@Nullable String applicationToken,
		@Nullable Path secureConnectBundle,
		@DefaultValue Map<String, RegionConfig> regions,
		@Nullable String primaryRegion,
		@DefaultValue FailoverProperties failover) {

	// ------------------------------------------------------------------ normalisation

	/**
	 * Returns a normalised region map that always contains at least one entry. If only the
	 * legacy single-region properties are set and {@code regions} is empty, the legacy values
	 * are promoted to a {@code "default"} entry. This lets downstream code always iterate over
	 * {@code regions()} without branching on the configuration style.
	 */
	public Map<String, RegionConfig> effectiveRegions() {
		if (regions != null && !regions.isEmpty()) {
			return regions;
		}
		// Backward-compat: promote legacy single-region bundle to a "default" entry.
		// The token is not stored in RegionConfig — it lives at this top level.
		if (secureConnectBundle != null) {
			Map<String, RegionConfig> promoted = new HashMap<>();
			promoted.put("default", new RegionConfig(secureConnectBundle, null));
			return promoted;
		}
		return regions != null ? regions : Map.of();
	}

	/**
	 * Resolves the effective primary region key. Resolution order:
	 * <ol>
	 *   <li>{@code ASTRA_PRIMARY_REGION} environment variable</li>
	 *   <li>{@link #primaryRegion()} from configuration</li>
	 *   <li>Auto-detected from the primary bundle's {@code datacenter.json}</li>
	 *   <li>First key in {@link #effectiveRegions()}</li>
	 * </ol>
	 */
	public String effectivePrimaryRegion() {
		// 1. Environment variable override.
		String envOverride = System.getenv("ASTRA_PRIMARY_REGION");
		if (hasText(envOverride)) {
			return envOverride;
		}
		// 2. Explicit config value.
		if (hasText(primaryRegion)) {
			return primaryRegion;
		}
		Map<String, RegionConfig> effective = effectiveRegions();
		if (effective.isEmpty()) {
			throw new IllegalStateException("No Astra DB regions are configured");
		}
		// 3. Auto-detect from the first bundle that carries a datacenter.json.
		for (Map.Entry<String, RegionConfig> entry : effective.entrySet()) {
			Optional<String> detected = detectRegionFromBundle(entry.getValue().secureConnectBundle());
			if (detected.isPresent()) {
				return detected.get();
			}
		}
		// 4. Fall back to the first configured key.
		return effective.keySet().iterator().next();
	}

	// ------------------------------------------------------------------ bundle metadata

	/**
	 * Reads the {@code datacenter.json} inside the secure connect bundle ZIP and returns the
	 * datacenter / region name, or {@link Optional#empty()} if the bundle is absent, unreadable,
	 * or does not contain the expected file.
	 *
	 * <p>The {@code datacenter.json} format used by Astra is:
	 * <pre>{@code { "region": "<region-name>", "datacenter": "<datacenter-name>", ... }}</pre>
	 * We check {@code "region"} first, then {@code "datacenter"} as fallback.
	 */
	public static Optional<String> detectRegionFromBundle(@Nullable Path bundle) {
		if (bundle == null || bundle.toString().isBlank() || !Files.isReadable(bundle)) {
			return Optional.empty();
		}
		try (ZipFile zip = new ZipFile(bundle.toFile())) {
			var entry = zip.getEntry("datacenter.json");
			if (entry == null) {
				return Optional.empty();
			}
			try (InputStream in = zip.getInputStream(entry)) {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode root = mapper.readTree(in);
				// Prefer "region"; fall back to "datacenter" for older bundle formats.
				for (String field : new String[] { "region", "datacenter" }) {
					JsonNode node = root.get(field);
					if (node != null && node.isTextual() && !node.asText().isBlank()) {
						return Optional.of(node.asText());
					}
				}
			}
		}
		catch (IOException e) {
			throw new UncheckedIOException("Failed to read datacenter.json from bundle: " + bundle, e);
		}
		return Optional.empty();
	}

	// ------------------------------------------------------------------ toString

	@Override
	public String toString() {
		// Never leak tokens into logs or actuator output.
		String tokenSummary = hasText(applicationToken) ? "******" : "<unset>";
		int regionCount = effectiveRegions().size();
		return "AstraDbProperties[applicationToken=" + tokenSummary
				+ ", secureConnectBundle=" + secureConnectBundle
				+ ", regions(" + regionCount + ")=" + effectiveRegions().keySet()
				+ ", primaryRegion=" + primaryRegion
				+ ", failover=" + failover + "]";
	}

	// ------------------------------------------------------------------ helpers

	private static boolean hasText(@Nullable String value) {
		return value != null && !value.isBlank();
	}

	// ------------------------------------------------------------------ nested types

	/**
	 * Per-region connection configuration.
	 *
	 * <p>The application token ({@code AstraCS:...}) is the same for all regions of an Astra
	 * database and is therefore held at the {@link AstraDbProperties} top level rather than
	 * repeated here.
	 *
	 * @param secureConnectBundle absolute path to the secure connect bundle ZIP for this region
	 * @param displayName         optional human-readable name shown in logs and metrics
	 */
	public record RegionConfig(
			@Nullable Path secureConnectBundle,
			@Nullable String displayName) {

		@Override
		public String toString() {
			return "RegionConfig[secureConnectBundle=" + secureConnectBundle
					+ ", displayName=" + displayName + "]";
		}
	}

	/**
	 * Failover policy settings ({@code astra.db.failover.*}).
	 *
	 * @param enabled          master switch — set to {@code false} to disable all failover logic
	 * @param maxAttempts      maximum number of standby regions to try; {@code 0} means unlimited
	 *                         (tries all remaining regions); defaults to {@code 0}
	 * @param readConsistency  consistency level used for <em>read</em> requests after failing
	 *                         over to a standby region; must be {@code LOCAL_QUORUM} or
	 *                         {@code LOCAL_ONE}; writes always stay at {@code LOCAL_QUORUM}
	 *                         (Astra-enforced)
	 */
	public record FailoverProperties(
			@DefaultValue("true") boolean enabled,
			@DefaultValue("0") int maxAttempts,
			@DefaultValue("LOCAL_QUORUM") String readConsistency) {
	}

}
