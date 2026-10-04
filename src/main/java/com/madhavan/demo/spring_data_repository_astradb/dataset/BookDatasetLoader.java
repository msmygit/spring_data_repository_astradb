package com.madhavan.demo.spring_data_repository_astradb.dataset;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import com.madhavan.demo.spring_data_repository_astradb.BooksProperties;
import com.madhavan.demo.spring_data_repository_astradb.book.Book;
import com.madhavan.demo.spring_data_repository_astradb.book.BookRepository;

import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * Reads the books dataset and writes it through {@link BookRepository#saveAll(Iterable)}.
 * <p>
 * {@code saveAll} issues one {@code INSERT} per book, sequentially. Instead of grouping rows from different partitions
 * into a {@code BATCH} (an anti-pattern in Cassandra: it overloads the coordinator), the list is split into chunks and
 * the chunks are written in parallel on virtual threads, with a {@link Semaphore} capping the number of in-flight
 * chunks. The {@code CqlSession} is thread-safe and multiplexes all requests over its connection pool.
 */
@Component
public class BookDatasetLoader {

	private static final Logger log = LoggerFactory.getLogger(BookDatasetLoader.class);

	private final BookRepository repository;

	private final JsonMapper jsonMapper;

	private final BooksProperties.Dataset settings;

	public BookDatasetLoader(BookRepository repository, JsonMapper jsonMapper, BooksProperties properties) {
		this.repository = repository;
		this.jsonMapper = jsonMapper;
		this.settings = properties.dataset();
	}

	public List<Book> read() {
		Resource location = this.settings.location();
		log.info("Reading books dataset from {}", location);
		try (InputStream in = location.getInputStream()) {
			List<DatasetBook> rows = this.jsonMapper.readValue(in, new TypeReference<List<DatasetBook>>() {
			});
			return rows.stream().map(DatasetBook::toBook).toList();
		}
		catch (IOException ex) {
			throw new IllegalStateException("Could not read books dataset from " + location, ex);
		}
	}

	/**
	 * @return {@code true} if the given books appear to be loaded already (first and last ISBN present)
	 */
	public boolean isLoaded(List<Book> books) {
		return !books.isEmpty() && this.repository.existsById(books.getFirst().isbn())
				&& this.repository.existsById(books.getLast().isbn());
	}

	public void save(List<Book> books) {
		int chunkSize = Math.max(1, this.settings.chunkSize());
		Semaphore inFlight = new Semaphore(Math.max(1, this.settings.concurrency()));
		AtomicInteger saved = new AtomicInteger();
		int progressStep = Math.max(chunkSize, books.size() / 10);
		long start = System.nanoTime();

		List<Future<?>> writes = new ArrayList<>();
		try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int from = 0; from < books.size(); from += chunkSize) {
				List<Book> chunk = books.subList(from, Math.min(from + chunkSize, books.size()));
				inFlight.acquire();
				writes.add(executor.submit(() -> {
					try {
						this.repository.saveAll(chunk);
						int total = saved.addAndGet(chunk.size());
						if (total / progressStep != (total - chunk.size()) / progressStep) {
							log.info("  ... {}/{} books saved", total, books.size());
						}
					}
					finally {
						inFlight.release();
					}
				}));
			}
			for (Future<?> write : writes) {
				write.get();
			}
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while saving books", ex);
		}
		catch (ExecutionException ex) {
			throw new IllegalStateException("Saving books failed", ex.getCause());
		}
		log.info("Saved {} books in {} ms", saved.get(), (System.nanoTime() - start) / 1_000_000);
	}

	/** Shape of one entry in {@code books_dataset.json}; note the dataset calls the column {@code year}. */
	record DatasetBook(String isbn, String title, String author, String genre, Integer year, String publisher,
			Integer pages) {

		Book toBook() {
			return new Book(this.isbn, this.title, this.author, this.genre, this.year, this.publisher, this.pages);
		}

	}

}
