package com.madhavan.demo.spring_data_repository_astradb.book;

import static org.springframework.data.cassandra.core.query.Criteria.where;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.UpdateOptions;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;

/**
 * Implementation of {@link BookRepositoryCustom}, built on the auto-configured {@link CassandraOperations}
 * ({@code CassandraTemplate}).
 */
class BookRepositoryCustomImpl implements BookRepositoryCustom {

	private final CassandraOperations operations;

	BookRepositoryCustomImpl(CassandraOperations operations) {
		this.operations = operations;
	}

	@Override
	public boolean insertIfNotExists(Book book) {
		return this.operations.insert(book, InsertOptions.builder().withIfNotExists().build()).wasApplied();
	}

	@Override
	public boolean updatePages(String isbn, int pages) {
		return this.operations.update(byIsbn(isbn), Update.empty().set("pages", pages), Book.class);
	}

	@Override
	public boolean updatePublisherIf(String isbn, String expectedPublisher, String newPublisher) {
		UpdateOptions onlyIfPublisherMatches = UpdateOptions.builder()
			.ifCondition(where("publisher").is(expectedPublisher))
			.build();
		Query query = byIsbn(isbn).queryOptions(onlyIfPublisherMatches);
		return this.operations.update(query, Update.empty().set("publisher", newPublisher), Book.class);
	}

	@Override
	public boolean deleteIfExists(String isbn) {
		Query query = byIsbn(isbn).queryOptions(DeleteOptions.builder().withIfExists().build());
		return this.operations.delete(query, Book.class);
	}

	private static Query byIsbn(String isbn) {
		return Query.query(where("isbn").is(isbn));
	}

}
