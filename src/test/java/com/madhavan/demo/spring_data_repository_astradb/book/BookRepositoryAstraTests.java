package com.madhavan.demo.spring_data_repository_astradb.book;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs against the Astra DB configured through {@code ASTRA_DB_*} environment variables; skipped when they are not
 * set. Uses its own author/genre values so it neither depends on nor disturbs the imported dataset.
 */
@SpringBootTest(properties = { "books.demo.enabled=false", "books.dataset.load=false",
		"astra.driver.reload.enabled=false" })
@EnabledIfEnvironmentVariable(named = "ASTRA_DB_APPLICATION_TOKEN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "ASTRA_DB_SECURE_BUNDLE_PATH", matches = ".+")
class BookRepositoryAstraTests {

	@Autowired
	private BookRepository books;

	private final String run = UUID.randomUUID().toString().substring(0, 8);

	private final String author = "it-author-" + this.run;

	private final String genre = "it-genre-" + this.run;

	private List<Book> fixtures;

	@BeforeEach
	void saveFixtures() {
		this.fixtures = List.of(book("1", 1999, 100), book("2", 2005, 200), book("3", 2010, 300));
		this.books.saveAll(this.fixtures);
	}

	@AfterEach
	void deleteFixtures() {
		this.books.deleteAllById(this.fixtures.stream().map(Book::isbn).toList());
		this.books.deleteIfExists(isbn("lwt"));
	}

	@Test
	void findByIdAndSaiIndexedColumns() {
		assertThat(this.books.findById(isbn("1"))).contains(this.fixtures.getFirst());
		assertThat(this.books.findByAuthor(this.author)).hasSize(3);
		assertThat(this.books.findByGenreAndPublishedYearGreaterThanEqual(this.genre, 2005)).extracting(Book::isbn)
			.containsExactlyInAnyOrder(isbn("2"), isbn("3"));
		assertThat(this.books.findGenreInDecade(this.genre, 2000, 2009)).extracting(Book::isbn)
			.containsExactly(isbn("2"));
		assertThat(this.books.countByGenre(this.genre)).isEqualTo(3);
		assertThat(this.books.existsByAuthor(this.author)).isTrue();
	}

	@Test
	void projectionAndPaging() {
		assertThat(this.books.findByAuthorAndGenre(this.author, this.genre)).extracting(BookSummary::getPublishedYear)
			.containsExactlyInAnyOrder(1999, 2005, 2010);

		Slice<Book> first = this.books.findByGenre(this.genre, PageRequest.of(0, 2));
		assertThat(first.getContent()).hasSize(2);
		assertThat(first.hasNext()).isTrue();
		Slice<Book> second = this.books.findByGenre(this.genre, first.nextPageable());
		assertThat(second.getContent()).hasSize(1);
	}

	@Test
	void updatesAndDeletes() {
		this.books.save(this.books.findById(isbn("1")).orElseThrow().withTitle("renamed"));
		this.books.updatePages(isbn("1"), 999);
		assertThat(this.books.findById(isbn("1"))).hasValueSatisfying(book -> {
			assertThat(book.title()).isEqualTo("renamed");
			assertThat(book.pages()).isEqualTo(999);
		});

		this.books.deleteById(isbn("1"));
		assertThat(this.books.existsById(isbn("1"))).isFalse();
	}

	@Test
	void lightweightTransactions() {
		Book lwt = book("lwt", 2020, 50);
		assertThat(this.books.insertIfNotExists(lwt)).isTrue();
		assertThat(this.books.insertIfNotExists(lwt)).isFalse();
		assertThat(this.books.updatePublisherIf(lwt.isbn(), "wrong", "new")).isFalse();
		assertThat(this.books.updatePublisherIf(lwt.isbn(), lwt.publisher(), "new")).isTrue();
		assertThat(this.books.findById(lwt.isbn())).hasValueSatisfying(b -> assertThat(b.publisher()).isEqualTo("new"));
		assertThat(this.books.deleteIfExists(lwt.isbn())).isTrue();
		assertThat(this.books.deleteIfExists(lwt.isbn())).isFalse();
	}

	private Book book(String suffix, int year, int pages) {
		return new Book(isbn(suffix), "Title " + suffix, this.author, this.genre, year, "IT Press", pages);
	}

	private String isbn(String suffix) {
		return "it-" + this.run + "-" + suffix;
	}

}
