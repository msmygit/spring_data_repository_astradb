package com.madhavan.demo.spring_data_repository_astradb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.core.io.Resource;

/**
 * Settings for the dataset import and the demo walkthrough ({@code books.*}).
 *
 * @param dataset dataset import settings
 * @param demo demo walkthrough settings
 */
@ConfigurationProperties("books")
public record BooksProperties(@DefaultValue Dataset dataset, @DefaultValue Demo demo) {

	/**
	 * @param load whether to import the dataset at start-up
	 * @param location where to read the JSON from: {@code https:}, {@code file:} or {@code classpath:}
	 * @param skipIfLoaded skip the import when the first and last dataset ISBNs are already present
	 * @param concurrency maximum number of {@code saveAll} chunks written in parallel
	 * @param chunkSize number of books per {@code saveAll} call
	 */
	public record Dataset(@DefaultValue("true") boolean load,
			@DefaultValue("https://raw.githubusercontent.com/mouraleonardo/books_dataset/development/books_dataset.json") Resource location,
			@DefaultValue("true") boolean skipIfLoaded, @DefaultValue("32") int concurrency,
			@DefaultValue("100") int chunkSize) {
	}

	/**
	 * @param enabled whether to run the query/update/delete walkthrough at start-up
	 */
	public record Demo(@DefaultValue("true") boolean enabled) {
	}

}
