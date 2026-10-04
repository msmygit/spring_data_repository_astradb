package com.madhavan.demo.spring_data_repository_astradb.book;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.SaiIndexed;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * A book, stored in the {@code books} table of the session keyspace ({@code spring.cassandra.keyspace-name}).
 * <p>
 * With {@code spring.cassandra.schema-action=create_if_not_exists} Spring Data derives the schema from this record:
 *
 * <pre class="code">
 * CREATE TABLE IF NOT EXISTS books (isbn text, author text, genre text, pages int, published_year int,
 *                                   publisher text, title text, PRIMARY KEY (isbn));
 * CREATE CUSTOM INDEX IF NOT EXISTS books_author_sai ON books (author) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
 * CREATE CUSTOM INDEX IF NOT EXISTS books_genre_sai ON books (genre) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
 * CREATE CUSTOM INDEX IF NOT EXISTS books_published_year_sai ON books (published_year) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
 * </pre>
 *
 * (Spring Data itself would render {@code USING 'sai'}, which Astra DB rejects; see {@code AstraSchemaCreator}.)
 *
 * Records are immutable, so changes are made with the {@code with*} methods and then saved.
 *
 * @param isbn partition key: every book is its own partition, so look-ups by ISBN are single-partition reads
 * @param title the title
 * @param author SAI-indexed: equality queries by author
 * @param genre SAI-indexed: equality queries by genre
 * @param publishedYear SAI-indexed: equality <em>and range</em> queries by year
 * @param publisher the publisher
 * @param pages number of pages
 */
@Table("books")
public record Book(
		@PrimaryKey("isbn") String isbn,
		@Column("title") String title,
		@SaiIndexed("books_author_sai") @Column("author") String author,
		@SaiIndexed("books_genre_sai") @Column("genre") String genre,
		@SaiIndexed("books_published_year_sai") @Column("published_year") Integer publishedYear,
		@Column("publisher") String publisher,
		@Column("pages") Integer pages) {

	public Book withTitle(String title) {
		return new Book(this.isbn, title, this.author, this.genre, this.publishedYear, this.publisher, this.pages);
	}

	public Book withPublisher(String publisher) {
		return new Book(this.isbn, this.title, this.author, this.genre, this.publishedYear, publisher, this.pages);
	}

	public Book withPages(Integer pages) {
		return new Book(this.isbn, this.title, this.author, this.genre, this.publishedYear, this.publisher, pages);
	}

}
