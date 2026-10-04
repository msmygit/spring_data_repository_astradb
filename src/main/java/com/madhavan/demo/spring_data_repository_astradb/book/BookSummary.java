package com.madhavan.demo.spring_data_repository_astradb.book;

/**
 * Closed interface projection: Spring Data only selects the columns backing these accessors instead of
 * {@code SELECT *}.
 */
public interface BookSummary {

	String getTitle();

	String getAuthor();

	Integer getPublishedYear();

}
