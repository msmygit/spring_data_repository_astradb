package com.madhavan.demo.spring_data_repository_astradb.astra;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.madhavan.demo.spring_data_repository_astradb.book.Book;

import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.SchemaFactory;
import org.springframework.data.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class AstraSchemaCreatorTests {

	@Test
	void saiIndexesAreRenderedAsCustomStorageAttachedIndexes() {
		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setInitialEntitySet(Set.of(Book.class));
		mappingContext.afterPropertiesSet();

		CassandraAdminOperations admin = mock(CassandraAdminOperations.class);
		given(admin.getSchemaFactory()).willReturn(new SchemaFactory(new MappingCassandraConverter(mappingContext)));
		AstraSchemaCreator creator = new AstraSchemaCreator(mappingContext, admin);
		List<String> cql = creator.createIndexSpecifications(true).stream().map(CreateIndexCqlGenerator::toCql).toList();

		assertThat(cql).hasSize(3)
			.allSatisfy(statement -> assertThat(statement).startsWith("CREATE CUSTOM INDEX IF NOT EXISTS books_")
				.contains("USING 'StorageAttachedIndex'"))
			.anySatisfy(statement -> assertThat(statement).contains("books_author_sai ON books (author)"))
			.anySatisfy(statement -> assertThat(statement).contains("books_genre_sai ON books (genre)"))
			.anySatisfy(
					statement -> assertThat(statement).contains("books_published_year_sai ON books (published_year)"));
	}

}
