package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Builds the Java driver's Typesafe {@link Config} from the Spring {@code Environment}.
 * <p>
 * Every property below {@value #ROOT} (e.g. {@code datastax-java-driver.basic.request.timeout}) is handed to the
 * driver verbatim, so the YAML mirrors the driver's {@code reference.conf} one-to-one. Precedence, highest first:
 * <ol>
 * <li>JVM system properties {@code -Ddatastax-java-driver.*} (Typesafe's own override mechanism)</li>
 * <li>Spring property sources, in normal Spring Boot order (command line, env vars, {@code ./config/application.yaml},
 * classpath {@code application.yaml}, ...)</li>
 * <li>the driver's built-in {@code reference.conf} defaults</li>
 * </ol>
 * The supplier is invoked once at start-up and again on every {@code DriverConfigLoader#reload()}, which is what makes
 * runtime changes possible.
 */
public final class SpringEnvironmentDriverConfigSupplier implements Supplier<Config> {

	/** Root path of all driver options; identical to the root used in the driver's own HOCON files. */
	public static final String ROOT = DefaultDriverConfigLoader.DEFAULT_ROOT_PATH;

	private static final Pattern LIST_ELEMENT = Pattern.compile("^(.+?)(?:\\[(\\d+)]|\\.(\\d+))$");

	private final ConfigurableEnvironment environment;

	public SpringEnvironmentDriverConfigSupplier(ConfigurableEnvironment environment) {
		this.environment = environment;
	}

	@Override
	public Config get() {
		ConfigFactory.invalidateCaches();
		Config fromSpring = ConfigFactory.parseMap(driverOptions(), "Spring Environment (" + ROOT + ".*)");
		return ConfigFactory.defaultOverrides()
			.withFallback(fromSpring)
			.withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
			.resolve()
			.getConfig(ROOT);
	}

	/**
	 * Current driver options as fully-qualified paths ({@code datastax-java-driver.basic.request.timeout -> "5s"}).
	 * List elements arrive from the binder as {@code key.0}, {@code key.1} (or {@code key[0]}) and are folded back
	 * into lists.
	 */
	public Map<String, Object> driverOptions() {
		Map<String, String> flat = Binder.get(this.environment)
			.bind(ROOT, Bindable.mapOf(String.class, String.class))
			.orElseGet(Map::of);
		Map<String, Object> options = new LinkedHashMap<>();
		Map<String, TreeMap<Integer, String>> lists = new LinkedHashMap<>();
		flat.forEach((key, value) -> {
			Matcher element = LIST_ELEMENT.matcher(key);
			if (element.matches()) {
				lists.computeIfAbsent(element.group(1), k -> new TreeMap<>())
					.put(Integer.parseInt(element.group(2) != null ? element.group(2) : element.group(3)), value);
			}
			else {
				options.put(ROOT + "." + key, value);
			}
		});
		lists.forEach((key, values) -> options.put(ROOT + "." + key, new ArrayList<>(values.values())));
		return options;
	}

}
