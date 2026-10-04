package com.madhavan.demo.spring_data_repository_astradb.astra;

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaDropper;
import org.springframework.data.cassandra.core.convert.CassandraConverter;

/**
 * {@link SessionFactoryFactoryBean} that applies {@code spring.cassandra.schema-action} exactly like the default one,
 * but creates SAI indexes through {@link AstraSchemaCreator} so that they work on Astra DB.
 */
class AstraSessionFactoryFactoryBean extends SessionFactoryFactoryBean {

	private CqlSession session;

	private CassandraConverter converter;

	@Override
	public void setSession(CqlSession session) {
		super.setSession(session);
		this.session = session;
	}

	@Override
	public void setConverter(CassandraConverter converter) {
		super.setConverter(converter);
		this.converter = converter;
	}

	@Override
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {
		CassandraAdminTemplate admin = new CassandraAdminTemplate(this.session, this.converter);
		if (drop) {
			CassandraPersistentEntitySchemaDropper dropper = new CassandraPersistentEntitySchemaDropper(
					this.converter.getMappingContext(), admin);
			dropper.dropTables(dropUnused);
			dropper.dropUserTypes(dropUnused);
		}
		AstraSchemaCreator creator = new AstraSchemaCreator(this.converter.getMappingContext(), admin);
		creator.createUserTypes(ifNotExists);
		creator.createTables(ifNotExists);
		creator.createIndexes(ifNotExists);
	}

}
