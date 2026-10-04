package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.util.List;

import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;

/**
 * Schema creator that renders {@code @SaiIndexed} indexes in the syntax Astra DB accepts.
 * <p>
 * Spring Data maps {@code @SaiIndexed} to the Apache Cassandra 5 shorthand {@code CREATE INDEX ... USING 'sai'}. Astra
 * DB rejects that ("Cannot specify index class for a non-CUSTOM index") and requires the explicit form
 * {@code CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'}, which Cassandra 5, DSE 6.9 and HCD accept as well.
 * Everything else (tables, user types, index names and SAI options) is left to Spring Data.
 */
class AstraSchemaCreator extends CassandraPersistentEntitySchemaCreator {

	static final String SAI_CLASS = "StorageAttachedIndex";

	AstraSchemaCreator(CassandraMappingContext mappingContext, CassandraAdminOperations adminOperations) {
		super(mappingContext, adminOperations);
	}

	@Override
	protected List<CreateIndexSpecification> createIndexSpecifications(boolean ifNotExists) {
		List<CreateIndexSpecification> specifications = super.createIndexSpecifications(ifNotExists);
		specifications.stream()
			.filter(index -> "sai".equalsIgnoreCase(index.getUsing()))
			.forEach(index -> index.using(SAI_CLASS)); // a non-"sai" class name marks the index as CUSTOM
		return specifications;
	}

}
