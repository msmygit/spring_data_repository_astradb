package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.boot.env.PropertySourceLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.Origin;
import org.springframework.boot.origin.TextResourceOrigin;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Hot-reloads Java driver options when an {@code application.yaml} / {@code application.properties} file on disk
 * changes.
 * <p>
 * At start-up it records every config file Spring Boot loaded from the file system (typically
 * {@code ./config/application.yaml}, {@code ./application.yaml}, anything passed via {@code spring.config.location} /
 * {@code spring.config.additional-location}, and the exploded classpath copy when running from an IDE). It then polls
 * their modification time; on a change it:
 * <ol>
 * <li>re-parses the file and swaps the corresponding property sources in the {@code Environment} in place (so
 * precedence is unchanged: env vars and command-line arguments still win), then</li>
 * <li>calls {@code DriverConfigLoader#reload()}, which re-runs {@link SpringEnvironmentDriverConfigSupplier} and makes
 * the driver fire a {@code ConfigChangeEvent} if anything changed, and</li>
 * <li>logs exactly which driver options changed, per execution profile.</li>
 * </ol>
 * Options the driver reads per request (timeouts, consistency, page size, idempotence, ...) apply to the very next
 * request; pool sizes are resized on the config change event; a few (contact points, load-balancing policy class,
 * protocol version, ...) are only read at start-up and need a restart.
 */
@Component
@ConditionalOnProperty(name = "astra.driver.reload.enabled", havingValue = "true", matchIfMissing = true)
public class DriverConfigReloader {

	private static final Logger log = LoggerFactory.getLogger(DriverConfigReloader.class);

	private static final String CONFIG_DATA_PREFIX = "Config resource '";

	private static final String DOCUMENT_SUFFIX_REGEX = " \\(document #\\d+\\)$";

	private final ConfigurableEnvironment environment;

	private final ObjectProvider<MultiRegionSessionManager> sessionManager;

	private final Map<Path, WatchedFile> watchedFiles = new LinkedHashMap<>();

	public DriverConfigReloader(ConfigurableEnvironment environment,
			ObjectProvider<MultiRegionSessionManager> sessionManager) {
		this.environment = environment;
		this.sessionManager = sessionManager;
		discoverConfigFiles();
	}

	private void discoverConfigFiles() {
		for (PropertySource<?> source : this.environment.getPropertySources()) {
			if (!(source instanceof OriginTrackedMapPropertySource tracked)
					|| !source.getName().startsWith(CONFIG_DATA_PREFIX) || tracked.getSource().isEmpty()) {
				continue;
			}
			Path file = fileOf(tracked);
			PropertySourceLoader loader = (file != null) ? loaderFor(file) : null;
			if (loader == null) {
				continue;
			}
			String baseName = source.getName().replaceFirst(DOCUMENT_SUFFIX_REGEX, "");
			this.watchedFiles.computeIfAbsent(file,
					f -> new WatchedFile(f, baseName, loader, lastModified(f)));
		}
		if (this.watchedFiles.isEmpty()) {
			log.info("Driver config hot-reload: no file-system config files found to watch");
		}
		else {
			log.info("Driver config hot-reload: watching {}", this.watchedFiles.keySet());
		}
	}

	@Scheduled(initialDelayString = "${astra.driver.reload.poll-interval:5s}",
			fixedDelayString = "${astra.driver.reload.poll-interval:5s}")
	public void pollForChanges() {
		boolean environmentChanged = false;
		for (Map.Entry<Path, WatchedFile> entry : this.watchedFiles.entrySet()) {
			WatchedFile watched = entry.getValue();
			FileTime modified = lastModified(watched.path());
			if (modified == null || Objects.equals(modified, watched.lastModified())) {
				continue;
			}
			entry.setValue(watched.withLastModified(modified));
			environmentChanged |= refreshPropertySources(watched);
		}
		if (environmentChanged) {
			reloadDriverConfig();
		}
	}

	private boolean refreshPropertySources(WatchedFile watched) {
		List<PropertySource<?>> reloaded;
		try {
			reloaded = watched.loader().load(watched.baseName(), new FileSystemResource(watched.path()));
		}
		catch (IOException | RuntimeException ex) {
			log.warn("Ignoring change to {}: it could not be parsed ({}). The previous configuration stays active.",
					watched.path(), ex.getMessage());
			return false;
		}
		MutablePropertySources sources = this.environment.getPropertySources();
		boolean replaced = false;
		for (PropertySource<?> source : reloaded) {
			if (sources.contains(source.getName())) {
				sources.replace(source.getName(), source);
				replaced = true;
			}
		}
		log.info("Detected change in {}", watched.path());
		return replaced;
	}

	/**
	 * Re-reads the driver configuration from the {@code Environment} and logs the effective
	 * differences for every managed session. Public so it can also be triggered
	 * programmatically (e.g. from an admin endpoint) after changing property sources.
	 */
	public void reloadDriverConfig() {
		MultiRegionSessionManager manager = this.sessionManager.getIfAvailable();
		if (manager == null) {
			return;
		}
		manager.getSessions().forEach((region, session) -> reloadSession(region, session));
	}

	private void reloadSession(String region, CqlSession session) {
		Map<String, Map<String, Object>> before = snapshot(session);
		try {
			Boolean changed = session.getContext()
					.getConfigLoader()
					.reload()
					.toCompletableFuture()
					.get(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS);
			if (!Boolean.TRUE.equals(changed)) {
				log.info("Driver configuration reloaded for region '{}': no options changed", region);
				return;
			}
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return;
		}
		catch (Exception ex) {
			log.warn("Driver configuration reload failed for region '{}'; previous config stays active", region, ex);
			return;
		}
		logDifferences(region, before, snapshot(session));
	}

	private static Map<String, Map<String, Object>> snapshot(CqlSession session) {
		Map<String, Map<String, Object>> profiles = new TreeMap<>();
		for (DriverExecutionProfile profile : session.getContext().getConfig().getProfiles().values()) {
			Map<String, Object> options = new TreeMap<>();
			profile.entrySet().forEach(e -> options.put(e.getKey(), e.getValue()));
			profiles.put(profile.getName(), options);
		}
		return profiles;
	}

	private static void logDifferences(String region,
			Map<String, Map<String, Object>> before, Map<String, Map<String, Object>> after) {
		StringBuilder changes = new StringBuilder();
		TreeSet<String> profiles = new TreeSet<>(before.keySet());
		profiles.addAll(after.keySet());
		for (String profile : profiles) {
			Map<String, Object> old = before.getOrDefault(profile, Map.of());
			Map<String, Object> now = after.getOrDefault(profile, Map.of());
			TreeSet<String> keys = new TreeSet<>(old.keySet());
			keys.addAll(now.keySet());
			for (String key : keys) {
				if (!Objects.equals(old.get(key), now.get(key))) {
					changes.append(System.lineSeparator())
							.append("  [")
							.append(profile)
							.append("] ")
							.append(key)
							.append(": ")
							.append(old.get(key))
							.append(" -> ")
							.append(now.get(key));
				}
			}
		}
		log.info("Driver configuration reloaded for region '{}', changed options:{}", region, changes);
	}

	private static Path fileOf(OriginTrackedMapPropertySource source) {
		String anyKey = source.getPropertyNames()[0];
		Origin origin = Origin.from(source.getOrigin(anyKey));
		while (origin != null && !(origin instanceof TextResourceOrigin)) {
			origin = origin.getParent();
		}
		if (origin instanceof TextResourceOrigin textOrigin && textOrigin.getResource() != null) {
			Resource resource = textOrigin.getResource();
			try {
				return resource.isFile() ? resource.getFile().toPath().toAbsolutePath().normalize() : null;
			}
			catch (IOException ex) {
				return null;
			}
		}
		return null;
	}

	private static PropertySourceLoader loaderFor(Path file) {
		String name = file.getFileName().toString();
		if (name.endsWith(".yaml") || name.endsWith(".yml")) {
			return new YamlPropertySourceLoader();
		}
		if (name.endsWith(".properties")) {
			return new PropertiesPropertySourceLoader();
		}
		return null;
	}

	private static FileTime lastModified(Path file) {
		try {
			return Files.getLastModifiedTime(file);
		}
		catch (IOException ex) {
			return null;
		}
	}

	private record WatchedFile(Path path, String baseName, PropertySourceLoader loader, FileTime lastModified) {

		WatchedFile withLastModified(FileTime modified) {
			return new WatchedFile(this.path, this.baseName, this.loader, modified);
		}

	}

}
