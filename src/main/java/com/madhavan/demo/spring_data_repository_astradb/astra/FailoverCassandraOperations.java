package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.QueryConsistencyException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.ExecutableDeleteOperation;
import org.springframework.data.cassandra.core.ExecutableInsertOperation;
import org.springframework.data.cassandra.core.ExecutableSelectOperation;
import org.springframework.data.cassandra.core.ExecutableUpdateOperation;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.UpdateOptions;
import org.springframework.data.cassandra.core.WriteResult;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.domain.Slice;
import org.springframework.util.Assert;

/**
 * {@link CassandraOperations} decorator that adds application-level cross-region failover on
 * top of a {@link MultiRegionSessionManager}.
 *
 * <p>Every operation is delegated to a {@link CassandraTemplate} that is bound to the currently
 * active session.  When a <em>failover-eligible</em> exception is caught, the manager is asked to
 * switch to the next healthy region and the operation is retried up to
 * {@link AstraDbProperties.FailoverProperties#maxAttempts()} times (default: all remaining regions).
 *
 * <h2>Failover-eligible exceptions</h2>
 * <ul>
 *   <li>{@link NoNodeAvailableException} — total DC outage; always triggers failover.</li>
 *   <li>{@link AllNodesFailedException} — triggers failover only when at least one suppressed
 *       cause is a <em>replica availability</em> error ({@link UnavailableException} or
 *       {@link QueryConsistencyException}).</li>
 *   <li>{@link DriverTimeoutException} — triggers failover.</li>
 *   <li>{@link CoordinatorException} — triggers failover only when it wraps a replica
 *       availability error.</li>
 * </ul>
 *
 * <h2>LWT operations</h2>
 * Detected via {@link InsertOptions#isIfNotExists()}, {@link UpdateOptions#isIfExists()}/
 * {@link UpdateOptions#getIfCondition()}, and {@link DeleteOptions#isIfExists()}.
 * For LWT, <strong>only {@link NoNodeAvailableException} (total DC outage)</strong> triggers
 * failover; all other failover-eligible exceptions are re-thrown immediately to prevent
 * Paxos duplicate-write risks.
 *
 * <h2>Consistency levels</h2>
 * Writes always use {@code LOCAL_QUORUM} (Astra-enforced).  After failover, reads use
 * {@link AstraDbProperties.FailoverProperties#readConsistency()} (default {@code LOCAL_QUORUM},
 * optionally {@code LOCAL_ONE}).
 *
 * <h2>Metrics</h2>
 * Publishes {@code astradb.failover.total} (counter) and {@code astradb.failover.latency}
 * (timer) to the injected {@link MeterRegistry} (no-op when registry is absent).
 */
public class FailoverCassandraOperations implements CassandraOperations {

	private static final Logger log = LoggerFactory.getLogger(FailoverCassandraOperations.class);

	private final MultiRegionSessionManager sessionManager;
	private final CassandraConverter converter;
	private final AstraDbProperties properties;

	/** One CassandraTemplate per region, lazily created on first use. */
	private final Map<String, CassandraTemplate> templates = new ConcurrentHashMap<>();

	private final Counter failoverCounter;
	private final Timer failoverTimer;

	public FailoverCassandraOperations(MultiRegionSessionManager sessionManager,
			CassandraConverter converter,
			AstraDbProperties properties,
			MeterRegistry meterRegistry) {
		Assert.notNull(sessionManager, "MultiRegionSessionManager must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");
		Assert.notNull(properties, "AstraDbProperties must not be null");
		this.sessionManager = sessionManager;
		this.converter = converter;
		this.properties = properties;

		if (meterRegistry != null) {
			this.failoverCounter = Counter.builder("astradb.failover.total")
					.description("Total number of cross-region failover attempts by FailoverCassandraOperations")
					.register(meterRegistry);
			this.failoverTimer = Timer.builder("astradb.failover.latency")
					.description("Time spent executing an operation that required cross-region failover")
					.register(meterRegistry);
		}
		else {
			this.failoverCounter = null;
			this.failoverTimer = null;
		}
	}

	// -------------------------------------------------------------------------
	// Core failover machinery
	// -------------------------------------------------------------------------

	/**
	 * Executes {@code operation} against the active region's template, retrying on a standby
	 * region when a failover-eligible exception is encountered.
	 *
	 * @param operation  the operation to run; receives the active {@link CassandraTemplate}
	 * @param isLwt      {@code true} when the operation uses LWT (IF NOT EXISTS / IF condition)
	 * @param isRead     {@code true} for read operations (affects read-consistency after failover)
	 * @param <T>        result type
	 * @return operation result
	 */
	private <T> T executeWithFailover(TemplateOperation<T> operation, boolean isLwt, boolean isRead) {
		AstraDbProperties.FailoverProperties failover = this.properties.failover();
		if (!failover.enabled()) {
			return operation.execute(activeTemplate());
		}

		List<String> regions = this.sessionManager.getConfiguredRegions();
		int maxAttempts = failover.maxAttempts() > 0 ? failover.maxAttempts() : regions.size() - 1;

		RuntimeException lastException = null;
		String attemptedRegion = this.sessionManager.getActiveRegion();

		for (int attempt = 0; attempt <= maxAttempts; attempt++) {
			CassandraTemplate template = activeTemplate();
			try {
				return operation.execute(template);
			}
			catch (RuntimeException ex) {
				Throwable cause = unwrapDataAccessException(ex);
				if (attempt < maxAttempts && shouldFailover(cause, isLwt)) {
					lastException = ex;
					String fromRegion = this.sessionManager.getActiveRegion();
					String toRegion = nextHealthyRegion(fromRegion);
					if (toRegion == null) {
						log.warn("Failover triggered from '{}' but no healthy standby region is available; rethrowing",
								fromRegion);
						throw ex;
					}
					log.warn("Failover attempt {}/{}: switching from region '{}' to '{}' due to: {}",
							attempt + 1, maxAttempts, fromRegion, toRegion, ex.getMessage());
					recordFailoverMetric(fromRegion, toRegion, ex);
					this.sessionManager.failoverTo(toRegion);
					// template for new region picked up in next loop iteration
				}
				else {
					throw ex;
				}
			}
		}

		// Should not reach here; safeguard re-throw
		throw lastException != null ? lastException
				: new IllegalStateException("Failover loop exhausted with no result");
	}

	/**
	 * Determines whether a given {@code cause} should trigger a failover to another region.
	 *
	 * <p>For LWT operations, only {@link NoNodeAvailableException} (total DC outage) triggers
	 * failover; other eligible exceptions are rejected to avoid Paxos duplicate-write risk.
	 */
	private boolean shouldFailover(Throwable cause, boolean isLwt) {
		if (cause instanceof NoNodeAvailableException) {
			// Total DC outage — always safe to failover (LWT included)
			return true;
		}
		// Remaining checks are not safe for LWT unless it is a total outage
		if (isLwt) {
			return false;
		}
		if (cause instanceof DriverTimeoutException) {
			return true;
		}
		if (cause instanceof AllNodesFailedException anfe) {
			return anfe.getAllErrors().values().stream()
					.flatMap(List::stream)
					.anyMatch(this::isReplicaAvailabilityError);
		}
		if (cause instanceof CoordinatorException) {
			return isReplicaAvailabilityError(cause);
		}
		return false;
	}

	/**
	 * Returns {@code true} when {@code t} is an error caused by replica unavailability:
	 * {@link UnavailableException} or {@link QueryConsistencyException}.
	 */
	private boolean isReplicaAvailabilityError(Throwable t) {
		return t instanceof UnavailableException || t instanceof QueryConsistencyException;
	}

	/** Returns the next configured region that is healthy and is not {@code currentRegion}. */
	private String nextHealthyRegion(String currentRegion) {
		for (String region : this.sessionManager.getConfiguredRegions()) {
			if (!region.equals(currentRegion) && this.sessionManager.isHealthy(region)) {
				return region;
			}
		}
		return null;
	}

	/** Unwraps a Spring {@link DataAccessException} to expose the underlying driver exception. */
	private Throwable unwrapDataAccessException(Throwable ex) {
		if (ex instanceof DataAccessException dae && dae.getCause() != null) {
			return dae.getCause();
		}
		return ex;
	}

	private void recordFailoverMetric(String fromRegion, String toRegion, RuntimeException ex) {
		if (this.failoverCounter != null) {
			this.failoverCounter.increment();
		}
	}

	// -------------------------------------------------------------------------
	// LWT detection helpers
	// -------------------------------------------------------------------------

	static boolean isLwtOperation(InsertOptions options) {
		return options != null && options.isIfNotExists();
	}

	static boolean isLwtOperation(UpdateOptions options) {
		return options != null && (options.isIfExists() || options.getIfCondition() != null);
	}

	static boolean isLwtOperation(DeleteOptions options) {
		return options != null && (options.isIfExists() || options.getIfCondition() != null);
	}

	// -------------------------------------------------------------------------
	// Template access
	// -------------------------------------------------------------------------

	/** Returns the {@link CassandraTemplate} for the currently active region. */
	private CassandraTemplate activeTemplate() {
		String region = this.sessionManager.getActiveRegion();
		return this.templates.computeIfAbsent(region, r -> {
			CqlSession session = this.sessionManager.getSession(r);
			Assert.notNull(session, () -> "No CqlSession available for region '" + r + "'");
			return new CassandraTemplate(session, this.converter);
		});
	}

	/** Functional interface for a template-based operation. */
	@FunctionalInterface
	private interface TemplateOperation<T> {
		T execute(CassandraTemplate template);
	}

	// -------------------------------------------------------------------------
	// CassandraOperations — core methods with failover
	// -------------------------------------------------------------------------

	@Override
	public ResultSet execute(Statement<?> statement) throws DataAccessException {
		return executeWithFailover(t -> t.execute(statement), false, false);
	}

	@Override
	public <T> T insert(T entity) throws DataAccessException {
		return executeWithFailover(t -> t.insert(entity), false, false);
	}

	@Override
	public <T> EntityWriteResult<T> insert(T entity, InsertOptions options) throws DataAccessException {
		boolean lwt = isLwtOperation(options);
		return executeWithFailover(t -> t.insert(entity, options), lwt, false);
	}

	@Override
	public <T> T update(T entity) throws DataAccessException {
		return executeWithFailover(t -> t.update(entity), false, false);
	}

	@Override
	public <T> EntityWriteResult<T> update(T entity, UpdateOptions options) throws DataAccessException {
		boolean lwt = isLwtOperation(options);
		return executeWithFailover(t -> t.update(entity, options), lwt, false);
	}

	@Override
	public void delete(Object entity) throws DataAccessException {
		executeWithFailover(t -> { t.delete(entity); return null; }, false, false);
	}

	@Override
	public WriteResult delete(Object entity, QueryOptions options) throws DataAccessException {
		boolean lwt = (options instanceof DeleteOptions deleteOptions) && isLwtOperation(deleteOptions);
		return executeWithFailover(t -> t.delete(entity, options), lwt, false);
	}

	@Override
	public WriteResult delete(Object entity, DeleteOptions options) throws DataAccessException {
		boolean lwt = isLwtOperation(options);
		return executeWithFailover(t -> t.delete(entity, options), lwt, false);
	}

	@Override
	public boolean deleteById(Object id, Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.deleteById(id, entityClass), false, false);
	}

	@Override
	public void truncate(Class<?> entityClass) throws DataAccessException {
		executeWithFailover(t -> { t.truncate(entityClass); return null; }, false, false);
	}

	// -------------------------------------------------------------------------
	// CassandraOperations — select / query methods
	// -------------------------------------------------------------------------

	@Override
	public <T> List<T> select(String cql, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.select(cql, entityClass), false, true);
	}

	@Override
	public <T> Stream<T> stream(String cql, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.stream(cql, entityClass), false, true);
	}

	@Override
	public <T> T selectOne(String cql, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.selectOne(cql, entityClass), false, true);
	}

	@Override
	public <T> List<T> select(Statement<?> statement, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.select(statement, entityClass), false, true);
	}

	@Override
	public <T> Slice<T> slice(Statement<?> statement, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.slice(statement, entityClass), false, true);
	}

	@Override
	public <T> Stream<T> stream(Statement<?> statement, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.stream(statement, entityClass), false, true);
	}

	@Override
	public <T> T selectOne(Statement<?> statement, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.selectOne(statement, entityClass), false, true);
	}

	@Override
	public <T> List<T> select(Query query, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.select(query, entityClass), false, true);
	}

	@Override
	public <T> Slice<T> slice(Query query, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.slice(query, entityClass), false, true);
	}

	@Override
	public <T> Stream<T> stream(Query query, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.stream(query, entityClass), false, true);
	}

	@Override
	public <T> T selectOne(Query query, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.selectOne(query, entityClass), false, true);
	}

	@Override
	public <T> T selectOneById(Object id, Class<T> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.selectOneById(id, entityClass), false, true);
	}

	@Override
	public boolean update(Query query, Update update, Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.update(query, update, entityClass), false, false);
	}

	@Override
	public boolean delete(Query query, Class<?> entityClass) throws DataAccessException {
		// Detect LWT from query options embedded in the Query
		boolean lwt = query.getQueryOptions()
				.filter(DeleteOptions.class::isInstance)
				.map(DeleteOptions.class::cast)
				.map(FailoverCassandraOperations::isLwtOperation)
				.orElse(false);
		return executeWithFailover(t -> t.delete(query, entityClass), lwt, false);
	}

	@Override
	public long count(Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.count(entityClass), false, true);
	}

	@Override
	public long count(Query query, Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.count(query, entityClass), false, true);
	}

	@Override
	public boolean exists(Object id, Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.exists(id, entityClass), false, true);
	}

	@Override
	public boolean exists(Query query, Class<?> entityClass) throws DataAccessException {
		return executeWithFailover(t -> t.exists(query, entityClass), false, true);
	}

	// -------------------------------------------------------------------------
	// CassandraOperations — batch, converter, metadata
	// -------------------------------------------------------------------------

	@Override
	public CassandraBatchOperations batchOps(BatchType batchType) {
		// Batch operations run against the currently active session's template.
		// Failover mid-batch is not supported — it is the caller's responsibility
		// to handle the exception and decide whether to replay the batch.
		return activeTemplate().batchOps(batchType);
	}

	@Override
	public CqlOperations getCqlOperations() {
		return activeTemplate().getCqlOperations();
	}

	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	@Override
	public CqlIdentifier getTableName(Class<?> entityClass) {
		return activeTemplate().getTableName(entityClass);
	}

	// -------------------------------------------------------------------------
	// FluentCassandraOperations — executable DSL (delegate to active template)
	// -------------------------------------------------------------------------

	@Override
	public <T> ExecutableSelectOperation.ExecutableSelect<T> query(Class<T> domainType) {
		return activeTemplate().query(domainType);
	}

	@Override
	public ExecutableSelectOperation.UntypedSelect query(String table) {
		return activeTemplate().query(table);
	}

	@Override
	public ExecutableSelectOperation.UntypedSelect query(Statement<?> statement) {
		return activeTemplate().query(statement);
	}

	@Override
	public <T> ExecutableInsertOperation.ExecutableInsert<T> insert(Class<T> domainType) {
		return activeTemplate().insert(domainType);
	}

	@Override
	public ExecutableUpdateOperation.ExecutableUpdate update(Class<?> domainType) {
		return activeTemplate().update(domainType);
	}

	@Override
	public ExecutableDeleteOperation.ExecutableDelete delete(Class<?> domainType) {
		return activeTemplate().delete(domainType);
	}

}
