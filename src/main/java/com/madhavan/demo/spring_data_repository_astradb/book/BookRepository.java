package com.madhavan.demo.spring_data_repository_astradb.book;

import java.util.List;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

/**
 * Spring Data repository for {@link Book}.
 * <p>
 * CRUD comes from {@link CassandraRepository}; the query methods below are derived from their names (or declared with
 * {@link Query}); partial updates and lightweight transactions come from the {@link BookRepositoryCustom} fragment.
 * <p>
 * Every non-key predicate used here is backed by an SAI index on {@code author}, {@code genre} or
 * {@code published_year}, so none of these queries needs {@code ALLOW FILTERING}. Queries combining several indexed
 * columns are intersected by SAI on the server.
 */
public interface BookRepository extends CassandraRepository<Book, String>, BookRepositoryCustom {

	// --- equality on a single SAI-indexed column ---------------------------------------------------------------

	List<Book> findByAuthor(String author);

	/** Paging in Cassandra is cursor based: a {@link Slice} carries the paging state for the next page. */
	Slice<Book> findByGenre(String genre, Pageable pageable);

	/** Large results can be streamed; the driver fetches pages lazily as the stream is consumed. */
	Stream<Book> streamByGenre(String genre);

	// --- range queries on a numeric SAI index ------------------------------------------------------------------

	/** Note: {@code Between} is exclusive in Spring Data Cassandra ({@code published_year > ? AND published_year < ?}). */
	List<Book> findByPublishedYearBetween(int fromExclusive, int toExclusive, Limit limit);

	// --- several SAI indexes combined (AND) --------------------------------------------------------------------

	List<Book> findByGenreAndPublishedYearGreaterThanEqual(String genre, int year);

	/** Projection: only {@code title}, {@code author} and {@code published_year} are read. */
	List<BookSummary> findByAuthorAndGenre(String author, String genre);

	// --- limiting, counting, existence -------------------------------------------------------------------------

	/** Per-method consistency level, overriding {@code datastax-java-driver.basic.request.consistency}. */
	@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
	List<Book> findTop5ByGenre(String genre);

	long countByGenre(String genre);

	boolean existsByAuthor(String author);

	// --- hand-written CQL --------------------------------------------------------------------------------------

	@Query("SELECT * FROM books WHERE genre = :genre AND published_year >= :from AND published_year <= :to")
	List<Book> findGenreInDecade(String genre, int from, int to);

}
