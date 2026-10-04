package com.madhavan.demo.spring_data_repository_astradb.book;

/**
 * Custom repository fragment for operations that are not expressible as derived queries: partial (column-level)
 * updates and lightweight transactions (LWT, {@code IF ...}).
 * <p>
 * Do not mix LWT and non-LWT writes on the same row: LWT writes are timestamped by Paxos on the server, so a later plain
 * {@code INSERT}/{@code UPDATE}/{@code DELETE} (client-side timestamp) may be silently shadowed. Rows managed with
 * {@code IF ...} should be updated and deleted with {@code IF ...} too.
 * <p>
 * Spring Data finds the implementation by naming convention: {@link BookRepositoryCustomImpl}.
 */
public interface BookRepositoryCustom {

	/**
	 * Insert only if no row with this ISBN exists ({@code INSERT ... IF NOT EXISTS}).
	 * @return {@code true} if the row was inserted
	 */
	boolean insertIfNotExists(Book book);

	/**
	 * Update a single column without reading or rewriting the rest of the row
	 * ({@code UPDATE books SET pages = ? WHERE isbn = ?}).
	 */
	boolean updatePages(String isbn, int pages);

	/**
	 * Compare-and-set the publisher ({@code UPDATE books SET publisher = ? WHERE isbn = ? IF publisher = ?}).
	 * @return {@code true} if the current publisher matched and the update was applied
	 */
	boolean updatePublisherIf(String isbn, String expectedPublisher, String newPublisher);

	/**
	 * Delete only if the row exists ({@code DELETE FROM books WHERE isbn = ? IF EXISTS}).
	 * @return {@code true} if a row was deleted
	 */
	boolean deleteIfExists(String isbn);

}
