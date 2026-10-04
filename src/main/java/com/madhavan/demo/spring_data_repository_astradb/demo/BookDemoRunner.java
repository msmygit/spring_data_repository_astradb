package com.madhavan.demo.spring_data_repository_astradb.demo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.madhavan.demo.spring_data_repository_astradb.BooksProperties;
import com.madhavan.demo.spring_data_repository_astradb.book.Book;
import com.madhavan.demo.spring_data_repository_astradb.book.BookRepository;
import com.madhavan.demo.spring_data_repository_astradb.book.BookSummary;
import com.madhavan.demo.spring_data_repository_astradb.dataset.BookDatasetLoader;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Component;

/**
 * End-to-end walkthrough, run once at start-up: verify schema, import the dataset, then query, update and delete
 * through {@link BookRepository}. Run with
 * {@code --logging.level.org.springframework.data.cassandra.core.cql.CqlTemplate=DEBUG} to see every CQL statement.
 */
@Component
@ConditionalOnProperty(name = "books.demo.enabled", havingValue = "true", matchIfMissing = true)
class BookDemoRunner implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(BookDemoRunner.class);

	private static final String AUTHOR = "David Smith";

	private static final String GENRE = "Science Fiction";

	/** Books with ISBNs outside the dataset, used for the create/update/delete steps. */
	private static final Book DEMO_BOOK = new Book("979-8-00000-001-0", "The Astra Chronicles", "Ada Cassandra",
			"Science Fiction", 2026, "Partition Press", 412);

	private static final Book LWT_BOOK = new Book("979-8-00000-004-1", "Consensus at Dawn", "Ada Cassandra",
			"Thriller", 2026, "Partition Press", 256);

	private static final List<Book> SEQUELS = List.of(
			new Book("979-8-00000-002-7", "Astra Returns", "Ada Cassandra", "Science Fiction", 2027, "Partition Press", 300),
			new Book("979-8-00000-003-4", "Astra Forever", "Ada Cassandra", "Science Fiction", 2028, "Partition Press", 280));

	private final BookRepository books;

	private final BookDatasetLoader datasetLoader;

	private final CqlSession session;

	private final BooksProperties properties;

	/** Wall-clock duration of each step, in execution order. */
	private final Map<String, Long> timings = new LinkedHashMap<>();

	BookDemoRunner(BookRepository books, BookDatasetLoader datasetLoader, CqlSession session,
			BooksProperties properties) {
		this.books = books;
		this.datasetLoader = datasetLoader;
		this.session = session;
		this.properties = properties;
	}

	@Override
	public void run(ApplicationArguments args) {
		long start = System.nanoTime();
		timed("1. Schema", this::showSchema);
		timed("2. Import dataset", this::loadDataset);
		timed("3. Queries", this::query);
		timed("4. Create / update / delete", this::createUpdateDelete);
		timed("5. Lightweight transactions", this::lightweightTransactions);
		step("Done in %d ms", millisSince(start));
		this.timings.forEach((name, millis) -> log.info("  {} {} ms", String.format("%-30s", name), millis));
	}

	private void timed(String name, Runnable step) {
		long start = System.nanoTime();
		step.run();
		long millis = millisSince(start);
		this.timings.put(name, millis);
		log.info("---- {} took {} ms", name, millis);
	}

	private static long millisSince(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	/** The table and SAI indexes were created at start-up by spring.cassandra.schema-action=create_if_not_exists. */
	private void showSchema() {
		CqlIdentifier keyspace = this.session.getKeyspace().orElseThrow();
		step("1. Schema (created by schema-action CREATE_IF_NOT_EXISTS) in keyspace '%s'", keyspace.asInternal());
		this.session.getMetadata()
			.getKeyspace(keyspace)
			.flatMap(ks -> ks.getTable("books"))
			.ifPresentOrElse(table -> {
				log.info("Table: {}", table.describe(false).replace('\n', ' '));
				for (IndexMetadata index : table.getIndexes().values()) {
					log.info("Index: {} on {} ({})", index.getName().asInternal(), index.getTarget(),
							index.getClassName().orElse(index.getKind().name()));
				}
				logPartitionKey(table);
			}, () -> log.warn("Table books not visible in driver metadata yet"));
	}

	private static void logPartitionKey(TableMetadata table) {
		log.info("Partition key: {}",
				table.getPartitionKey().stream().map(column -> column.getName().asInternal()).toList());
	}

	private void loadDataset() {
		BooksProperties.Dataset dataset = this.properties.dataset();
		step("2. Import dataset with BookRepository.saveAll(..)");
		if (!dataset.load()) {
			log.info("Skipped (books.dataset.load=false)");
			return;
		}
		long start = System.nanoTime();
		List<Book> all = this.datasetLoader.read();
		log.info("Read {} books from the dataset in {} ms", all.size(), millisSince(start));
		if (dataset.skipIfLoaded() && this.datasetLoader.isLoaded(all)) {
			log.info("Dataset already present; skipping import (books.dataset.skip-if-loaded=true)");
			return;
		}
		this.datasetLoader.save(all);
	}

	private void query() {
		step("3. Queries");

		Book any = this.books.findByAuthor(AUTHOR).getFirst();
		log.info("findById('{}') -> {}", any.isbn(), this.books.findById(any.isbn()).orElseThrow());

		List<Book> byAuthor = this.books.findByAuthor(AUTHOR);
		log.info("findByAuthor('{}') -> {} books", AUTHOR, byAuthor.size());
		byAuthor.forEach(book -> log.info("    {}", book));

		Slice<Book> page = this.books.findByGenre(GENRE, PageRequest.of(0, 3));
		log.info("findByGenre('{}', page 1 of size 3) -> {}", GENRE, titles(page.getContent()));
		if (page.hasNext()) {
			Slice<Book> next = this.books.findByGenre(GENRE, page.nextPageable());
			log.info("findByGenre('{}', page 2 via paging state) -> {}", GENRE, titles(next.getContent()));
		}

		// Spring Data Cassandra renders Between with exclusive bounds: published_year > ? AND published_year < ?
		List<Book> nineties = this.books.findByPublishedYearBetween(1989, 2000, Limit.of(5));
		log.info("findByPublishedYearBetween(1989, 2000, Limit.of(5)) -> {}",
				nineties.stream().map(b -> b.title() + " (" + b.publishedYear() + ")").toList());

		List<Book> recentSciFi = this.books.findByGenreAndPublishedYearGreaterThanEqual(GENRE, 2020);
		log.info("findByGenreAndPublishedYearGreaterThanEqual('{}', 2020) -> {} books", GENRE, recentSciFi.size());

		List<BookSummary> summaries = this.books.findByAuthorAndGenre(AUTHOR, "Historical");
		log.info("findByAuthorAndGenre('{}', 'Historical') [projection] -> {}", AUTHOR, summaries.stream()
			.map(s -> s.getTitle() + " by " + s.getAuthor() + " (" + s.getPublishedYear() + ")")
			.toList());

		log.info("findTop5ByGenre('Horror') [@Consistency(LOCAL_ONE)] -> {}",
				titles(this.books.findTop5ByGenre("Horror")));

		log.info("findGenreInDecade('Mystery', 1950, 1959) [@Query] -> {} books",
				this.books.findGenreInDecade("Mystery", 1950, 1959).size());

		try (Stream<Book> stream = this.books.streamByGenre("Romance")) {
			log.info("streamByGenre('Romance') -> max pages = {}",
					stream.mapToInt(Book::pages).max().orElse(0));
		}

		log.info("countByGenre('{}') -> {}", GENRE, this.books.countByGenre(GENRE));
		log.info("existsByAuthor('{}') -> {}, existsByAuthor('Nobody') -> {}", AUTHOR,
				this.books.existsByAuthor(AUTHOR), this.books.existsByAuthor("Nobody"));
	}

	private void createUpdateDelete() {
		step("4. Create / update / delete");
		String isbn = DEMO_BOOK.isbn();

		// save() is an upsert: INSERT of the whole row.
		this.books.save(DEMO_BOOK);
		log.info("save(new book)               -> existsById = {}", this.books.existsById(isbn));

		// Read-modify-write: change a field on the immutable record and save the whole row again.
		Book renamed = this.books.findById(isbn).orElseThrow().withTitle("The Astra Chronicles, 2nd ed.");
		this.books.save(renamed);
		log.info("save(withTitle(..))          -> {}", this.books.findById(isbn).orElseThrow());

		// Partial update: touches one column, no read needed.
		this.books.updatePages(isbn, 450);
		log.info("updatePages(450)             -> {}", this.books.findById(isbn).orElseThrow());

		// Bulk save, query through the author index, bulk delete by id.
		this.books.saveAll(SEQUELS);
		log.info("saveAll(sequels)             -> findByAuthor('Ada Cassandra') = {}",
				titles(this.books.findByAuthor("Ada Cassandra")));
		this.books.deleteAllById(SEQUELS.stream().map(Book::isbn).toList());
		this.books.deleteById(isbn);
		log.info("deleteAllById(..), deleteById(..) -> existsByAuthor('Ada Cassandra') = {}",
				this.books.existsByAuthor("Ada Cassandra"));
	}

	private void lightweightTransactions() {
		step("5. Lightweight transactions (compare-and-set)");
		// A separate row: rows written with IF ... must keep using IF ... (see BookRepositoryCustom).
		String lwtIsbn = LWT_BOOK.isbn();
		log.info("insertIfNotExists(new)       -> applied={}", this.books.insertIfNotExists(LWT_BOOK));
		log.info("insertIfNotExists(duplicate) -> applied={}", this.books.insertIfNotExists(LWT_BOOK));
		log.info("updatePublisherIf(expected='Wrong Press')     -> applied={}",
				this.books.updatePublisherIf(lwtIsbn, "Wrong Press", "Token Ring Books"));
		log.info("updatePublisherIf(expected='Partition Press') -> applied={}",
				this.books.updatePublisherIf(lwtIsbn, "Partition Press", "Token Ring Books"));
		log.info("after conditional update     -> {}", this.books.findById(lwtIsbn).orElseThrow());
		log.info("deleteIfExists(..)           -> applied={}, existsById = {}", this.books.deleteIfExists(lwtIsbn),
				this.books.existsById(lwtIsbn));
	}

	private static List<String> titles(List<Book> books) {
		return books.stream().map(Book::title).toList();
	}

	private static void step(String title, Object... args) {
		log.info("");
		log.info("==== {} ====", String.format(title, args));
	}

}
