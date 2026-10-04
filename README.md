# Spring Data Cassandra repositories on DataStax Astra DB

A reference implementation that shows how to talk to **DataStax Astra DB** with **Spring Data Cassandra
repositories** on Spring Boot 4 / Java 25. It covers:

| Topic | Where |
|---|---|
| Connecting with a secure connect bundle + application token, secrets only from env vars | [`AstraCassandraConfiguration`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/AstraCassandraConfiguration.java), [`AstraDbProperties`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/AstraDbProperties.java) |
| Any Java driver option configurable in `application.yaml`, **hot-reloaded at runtime** | [`SpringEnvironmentDriverConfigSupplier`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/SpringEnvironmentDriverConfigSupplier.java), [`DriverConfigReloader`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/DriverConfigReloader.java) |
| `CREATE TABLE IF NOT EXISTS` + SAI indexes derived from the entity mapping | [`Book`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/book/Book.java), [`AstraSchemaCreator`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/AstraSchemaCreator.java) |
| Derived queries, `@Query`, paging, streaming, projections, `@Consistency`, limiting, count/exists | [`BookRepository`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/book/BookRepository.java) |
| Custom repository fragment: partial updates, lightweight transactions (LWT) | [`BookRepositoryCustom`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/book/BookRepositoryCustom.java), [`BookRepositoryCustomImpl`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/book/BookRepositoryCustomImpl.java) |
| Bulk import of 10,000 books through `saveAll`, in parallel on virtual threads | [`BookDatasetLoader`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/dataset/BookDatasetLoader.java) |
| End-to-end walkthrough (schema → import → query → update → delete) | [`BookDemoRunner`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/demo/BookDemoRunner.java) |

Stack: Spring Boot 4.1.1 · Spring Data Cassandra 5.1.1 · Apache Cassandra Java driver 4.19.3 · Java 25.

---

## 1. Prerequisites

1. **Java 25** (the Maven wrapper downloads Maven itself).
2. An **Astra DB Serverless** database, from <https://astra.datastax.com>.
   * New databases come with a keyspace called `default_keyspace`. To use a different keyspace, create it in the
     Astra UI or CLI first: Astra does not allow `CREATE KEYSPACE` over CQL.
3. An **application token** with a role that can create tables and indexes (for example *Database Administrator*).
   It looks like `AstraCS:...`.
4. The database's **secure connect bundle** (`secure-connect-<db>.zip`): *Database → Connect → Drivers → Download
   bundle*. It contains the contact points, local datacenter and mTLS certificates.

## 2. Configure: secrets come from environment variables only

```bash
export ASTRA_DB_APPLICATION_TOKEN='AstraCS:...'
export ASTRA_DB_SECURE_BUNDLE_PATH=/absolute/path/to/secure-connect-<db>.zip
export ASTRA_DB_KEYSPACE=default_keyspace   # optional; this is the default
```

[`application.yaml`](src/main/resources/application.yaml) only *references* these variables:

```yaml
spring:
  cassandra:
    keyspace-name: ${ASTRA_DB_KEYSPACE:default_keyspace}
    schema-action: create_if_not_exists
astra:
  db:
    application-token: ${ASTRA_DB_APPLICATION_TOKEN:}
    secure-connect-bundle: ${ASTRA_DB_SECURE_BUNDLE_PATH:}
```

* No token or bundle is ever committed. `.gitignore` excludes `*.zip`, `.env` and the local `config/` override folder.
* `AstraDbProperties#toString()` masks the token, so it can't end up in logs.
* If a variable is missing, startup fails immediately with an error naming the variable, e.g.
  `ASTRA_DB_APPLICATION_TOKEN is not set`.

## 3. Run it end to end

```bash
./mvnw spring-boot:run
```

or as a jar:

```bash
./mvnw -DskipTests package
java -jar target/spring_data_repository_astradb-0.0.1-SNAPSHOT.jar
```

The first run:

1. connects to Astra and creates the `books` table and three SAI indexes if they don't exist;
2. downloads the [books dataset](https://github.com/mouraleonardo/books_dataset) and imports all 10,000 books with
   `BookRepository.saveAll(..)` (about 13 seconds on a laptop);
3. runs the query, update, delete and LWT walkthrough, then exits.

Later runs notice the dataset is already present and skip the import (`books.dataset.skip-if-loaded=true`).

Abridged output:

```text
==== 1. Schema (created by schema-action CREATE_IF_NOT_EXISTS) in keyspace 'default_keyspace' ====
Table: CREATE TABLE "default_keyspace"."books" ( "isbn" text, "author" text, "genre" text, "pages" int, "published_year" int, "publisher" text, "title" text, PRIMARY KEY ("isbn") ) WITH ...
Index: books_author_sai on author (StorageAttachedIndex)
Index: books_genre_sai on genre (StorageAttachedIndex)
Index: books_published_year_sai on published_year (StorageAttachedIndex)
Partition key: [isbn]
---- 1. Schema took 2 ms

==== 2. Import dataset with BookRepository.saveAll(..) ====
Read 10000 books from the dataset in 850 ms
  ... 1000/10000 books saved
  ...
Saved 10000 books in 13023 ms
---- 2. Import dataset took 14102 ms

==== 3. Queries ====
findById('978-1-03-338693-4') -> Book[isbn=978-1-03-338693-4, title=Beat myself lawyer size, author=David Smith, ...]
findByAuthor('David Smith') -> 9 books
findByGenre('Science Fiction', page 1 of size 3) -> [Coach buy gas stand, Manager campaign put, Ask share]
findByGenre('Science Fiction', page 2 via paging state) -> [Where store order piece, Special best rather, Leave]
findByPublishedYearBetween(1989, 2000, Limit.of(5)) -> [Coach buy gas stand (1995), Art (1998), ...]
findByGenreAndPublishedYearGreaterThanEqual('Science Fiction', 2020) -> 34 books
findByAuthorAndGenre('David Smith', 'Historical') [projection] -> [Beat myself lawyer size by David Smith (1907), ...]
findTop5ByGenre('Horror') [@Consistency(LOCAL_ONE)] -> [Agree, Land yard five worker, ...]
findGenreInDecade('Mystery', 1950, 1959) [@Query] -> 74 books
streamByGenre('Romance') -> max pages = 1000
countByGenre('Science Fiction') -> 1045
existsByAuthor('David Smith') -> true, existsByAuthor('Nobody') -> false
---- 3. Queries took 1840 ms

==== 4. Create / update / delete ====
save(new book)               -> existsById = true
save(withTitle(..))          -> Book[isbn=979-8-00000-001-0, title=The Astra Chronicles, 2nd ed., ..., pages=412]
updatePages(450)             -> Book[isbn=979-8-00000-001-0, title=The Astra Chronicles, 2nd ed., ..., pages=450]
saveAll(sequels)             -> findByAuthor('Ada Cassandra') = [Astra Forever, Astra Returns, The Astra Chronicles, 2nd ed.]
deleteAllById(..), deleteById(..) -> existsByAuthor('Ada Cassandra') = false
---- 4. Create / update / delete took 410 ms

==== 5. Lightweight transactions (compare-and-set) ====
insertIfNotExists(new)       -> applied=true
insertIfNotExists(duplicate) -> applied=false
updatePublisherIf(expected='Wrong Press')     -> applied=false
updatePublisherIf(expected='Partition Press') -> applied=true
deleteIfExists(..)           -> applied=true, existsById = false
---- 5. Lightweight transactions took 520 ms

==== Done in 16874 ms ====
  1. Schema                      2 ms
  2. Import dataset              14102 ms
  3. Queries                     1840 ms
  4. Create / update / delete    410 ms
  5. Lightweight transactions    520 ms
```

Timings are illustrative; they depend on your network and database region.

Useful switches (any Spring property works as `--name=value`):

| Property | Default | Purpose |
|---|---|---|
| `books.dataset.load` | `true` | Import the dataset at startup |
| `books.dataset.location` | GitHub raw URL | `https:`, `file:` or `classpath:` location of the JSON |
| `books.dataset.skip-if-loaded` | `true` | Skip the import if the first and last ISBNs already exist |
| `books.dataset.concurrency` / `chunk-size` | `32` / `100` | Number of parallel `saveAll` chunks / books per chunk |
| `books.demo.enabled` | `true` | Run the walkthrough |
| `spring.main.keep-alive` | `false` | Keep running after the walkthrough (e.g. to try hot-reload) |
| `astra.driver.reload.enabled` / `poll-interval` | `true` / `5s` | Driver config hot-reload |

## 4. See every CQL statement: `CqlTemplate=DEBUG`

All repository and template operations end up in Spring Data's `CqlTemplate`, which logs each statement at
`DEBUG`. Turn it on without changing code:

```bash
# Maven
./mvnw spring-boot:run \
  -Dspring-boot.run.arguments="--logging.level.org.springframework.data.cassandra.core.cql.CqlTemplate=DEBUG"

# Jar
java -jar target/spring_data_repository_astradb-0.0.1-SNAPSHOT.jar \
  --logging.level.org.springframework.data.cassandra.core.cql.CqlTemplate=DEBUG

# Environment variable. Relaxed binding lowercases env-var logger names, which breaks class-level loggers such as
# CqlTemplate, so use SPRING_APPLICATION_JSON:
SPRING_APPLICATION_JSON='{"logging.level.org.springframework.data.cassandra.core.cql.CqlTemplate":"DEBUG"}' \
  ./mvnw spring-boot:run
```

…or uncomment the line in `application.yaml`. Tip: also pass `--books.dataset.load=false` so you don't log 10,000
`INSERT`s. The output shows exactly what each repository method generates:

```text
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [SELECT * FROM books WHERE author=?]
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [SELECT * FROM books WHERE published_year>? AND published_year<? LIMIT ?]
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [SELECT title,author,published_year FROM books WHERE author=? AND genre=?]
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [UPDATE books SET pages=? WHERE isbn=?]
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [UPDATE books SET publisher=? WHERE isbn=? IF publisher=?]
DEBUG o.s.data.cassandra.core.cql.CqlTemplate : Executing prepared statement [DELETE FROM books WHERE isbn IN ?]
```

Each call also logs `Preparing statement [...]`. That's expected: Spring Data asks the session to prepare every time,
but the driver caches prepared statements, so only the first call makes a network round trip.

## 5. Schema: entity → `CREATE TABLE IF NOT EXISTS` + SAI

```java
@Table("books")
public record Book(
        @PrimaryKey("isbn") String isbn,                                      // partition key
        @Column("title") String title,
        @SaiIndexed("books_author_sai") @Column("author") String author,
        @SaiIndexed("books_genre_sai") @Column("genre") String genre,
        @SaiIndexed("books_published_year_sai") @Column("published_year") Integer publishedYear,
        @Column("publisher") String publisher,
        @Column("pages") Integer pages) { ... }
```

With `spring.cassandra.schema-action: create_if_not_exists`, startup runs:

```sql
CREATE TABLE IF NOT EXISTS books (isbn text, author text, genre text, pages int, published_year int,
                                  publisher text, title text, PRIMARY KEY (isbn));
CREATE CUSTOM INDEX IF NOT EXISTS books_author_sai ON books (author) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
CREATE CUSTOM INDEX IF NOT EXISTS books_genre_sai ON books (genre) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
CREATE CUSTOM INDEX IF NOT EXISTS books_published_year_sai ON books (published_year) USING 'StorageAttachedIndex' WITH OPTIONS = {...};
```

The table goes into whatever keyspace the session uses (`ASTRA_DB_KEYSPACE` / `spring.cassandra.keyspace-name`), so the
entity has no hard-coded keyspace.

> **Astra gotcha:** on its own, Spring Data turns `@SaiIndexed` into the Cassandra 5 shorthand
> `CREATE INDEX ... USING 'sai'`. Astra rejects that with *"Cannot specify index class for a non-CUSTOM index"*.
> [`AstraSchemaCreator`](src/main/java/com/madhavan/demo/spring_data_repository_astradb/astra/AstraSchemaCreator.java)
> keeps the annotation-driven approach but generates `CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'`, which
> Cassandra 5, DSE 6.9 and HCD accept as well. It's wired in through a `SessionFactoryFactoryBean` subclass, and Spring
> Boot's auto-configured factory backs off.

`schema-action` is convenient for demos and development. In production, most teams manage DDL with migrations or
infrastructure-as-code and set `schema-action: none`.

## 6. Repository walkthrough

`BookRepository extends CassandraRepository<Book, String>, BookRepositoryCustom`:

| Method | Generated CQL | Shows |
|---|---|---|
| `findById`, `existsById`, `save`, `saveAll`, `deleteById`, `deleteAllById` | `SELECT … WHERE isbn=?`, `INSERT …`, `DELETE … WHERE isbn IN ?` | Inherited CRUD (`save` is an upsert) |
| `findByAuthor(author)` | `SELECT * FROM books WHERE author=?` | Equality on an SAI column |
| `findByGenre(genre, Pageable)` → `Slice` | `SELECT * FROM books WHERE genre=?` + paging state | Cursor paging (`slice.nextPageable()`) |
| `streamByGenre(genre)` → `Stream` | `SELECT * FROM books WHERE genre=?` | Pages fetched lazily as the stream is consumed |
| `findByPublishedYearBetween(from, to, Limit)` | `… WHERE published_year>? AND published_year<? LIMIT ?` | Numeric SAI range. **`Between` is exclusive in Spring Data Cassandra** |
| `findByGenreAndPublishedYearGreaterThanEqual` | `… WHERE genre=? AND published_year>=?` | Intersecting two SAI indexes, no `ALLOW FILTERING` |
| `findByAuthorAndGenre` → `List<BookSummary>` | `SELECT title,author,published_year …` | Interface projection reads only the columns it needs |
| `@Consistency(LOCAL_ONE) findTop5ByGenre` | `… WHERE genre=? LIMIT ?` | Per-method consistency, result limiting |
| `countByGenre`, `existsByAuthor` | `SELECT count(1) …`, `… LIMIT 1` | Count and existence queries |
| `@Query findGenreInDecade` | your CQL with named parameters | Hand-written CQL |

The custom fragment (`BookRepositoryCustomImpl`, built on `CassandraOperations`) adds:

| Method | CQL |
|---|---|
| `updatePages(isbn, pages)` | `UPDATE books SET pages=? WHERE isbn=?` (partial update, no read first) |
| `insertIfNotExists(book)` | `INSERT … IF NOT EXISTS` |
| `updatePublisherIf(isbn, expected, new)` | `UPDATE books SET publisher=? WHERE isbn=? IF publisher=?` |
| `deleteIfExists(isbn)` | `DELETE FROM books WHERE isbn=? IF EXISTS` |

Data-modeling notes:

* `isbn` is the partition key, so `findById` is a single-partition read. Every other predicate is served by an SAI
  index, so none of the queries need `ALLOW FILTERING`.
* SAI queries without a partition key fan out across the cluster. Use them for selective, secondary access paths, not
  as a substitute for a primary key that matches your main read pattern.
* `countByGenre` makes Astra return the warning `Aggregation query used without partition key`. The demo keeps that
  warning visible on purpose: counting across partitions gets expensive as data grows.
* **Don't mix LWT and non-LWT writes on the same row.** LWT writes are timestamped by Paxos on the server, so a later
  plain `save`/`UPDATE`/`DELETE`, which uses a client-side timestamp, can be silently discarded. That's why the demo
  runs its LWT steps on a separate book and removes it with `deleteIfExists`.
* `Book` is an immutable record. Changes go through `withTitle(..)`/`withPages(..)` and then `save(..)`, or use a
  partial `UPDATE` from the fragment.

### Bulk loading

`saveAll` issues one `INSERT` per entity. The loader splits the 10,000 books into chunks and runs
`repository.saveAll(chunk)` concurrently on **virtual threads**, with a `Semaphore` limiting how many chunks are in
flight (`books.dataset.concurrency`). It deliberately avoids `BATCH`: a batch spanning many partitions overloads the
coordinator and is an anti-pattern in Cassandra.

## 7. Java driver configuration in `application.yaml`, reloaded at runtime

Spring Boot's own `spring.cassandra.*` properties map only a subset of driver options, and they're fixed at startup.
This project replaces Boot's `DriverConfigLoader` so that **any** option from the driver's
[reference.conf](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/)
can be set under `datastax-java-driver:`, using the same path as in `reference.conf`:

```yaml
datastax-java-driver:
  basic:
    request:
      timeout: 10 seconds
      consistency: LOCAL_QUORUM
      page-size: 5000
  advanced:
    connection:
      pool:
        remote:
          size: 2
  profiles:                 # execution profiles work as well
    slow:
      basic.request.timeout: 30 seconds
```

Options are resolved with normal Spring precedence, highest first:

1. `-Ddatastax-java-driver.…` JVM system properties
2. command-line arguments (`--datastax-java-driver.basic.request.timeout=5s`)
3. `./config/application.yaml`, then `./application.yaml`, profile-specific files, …
4. `src/main/resources/application.yaml`
5. the driver's built-in defaults

Contact points, local datacenter and TLS come from the secure connect bundle, so don't set them here.

### Changing options while the app runs

On startup, `DriverConfigReloader` records every config file Spring Boot loaded from disk and checks their modification
time every `astra.driver.reload.poll-interval`. When a file changes, it:

1. reparses the file and replaces its property sources in the Spring `Environment`, so precedence stays the same;
2. calls `DriverConfigLoader.reload()`, which reads the driver config from the `Environment` again; the driver then
   applies the change and fires a `ConfigChangeEvent`;
3. logs exactly which options changed.

To try it:

```bash
mkdir -p config
cat > config/application.yaml <<'EOF'
datastax-java-driver:
  basic:
    request:
      timeout: 10 seconds
EOF

./mvnw spring-boot:run -Dspring-boot.run.arguments="--spring.main.keep-alive=true --books.dataset.load=false"
```

While it's running, edit `config/application.yaml`:

```yaml
datastax-java-driver:
  basic:
    request:
      timeout: 3 seconds
      consistency: LOCAL_ONE
      page-size: 500
  advanced:
    connection:
      pool:
        remote:
          size: 2
```

Within a few seconds the log shows:

```text
INFO  DriverConfigReloader    : Detected change in /…/config/application.yaml
INFO  DefaultDriverConfigLoader : [s0] Detected a configuration change
INFO  DriverConfigReloader    : Driver configuration reloaded, changed options:
  [default] advanced.connection.pool.remote.size: 1 -> 2
  [default] basic.request.consistency: LOCAL_QUORUM -> LOCAL_ONE
  [default] basic.request.page-size: 5000 -> 500
  [default] basic.request.timeout: 10 seconds -> 3 seconds
```

* If you delete a key from the file, that option falls back to the next source (here the classpath `application.yaml`).
* If you save invalid YAML, it's ignored with a warning and the previous configuration stays active.
* Options the driver reads per request take effect on the next request: timeouts, consistency, page size,
  idempotence, and so on. Pool sizes are resized when the driver receives the change event. A few options, such as the
  load-balancing policy class or the protocol version, are only read at startup and need a restart.
* When running from the IDE or `spring-boot:run`, `target/classes/application.yaml` is watched as well.
  `./config/application.yaml` is the most convenient file to edit.
* `@Consistency` on a repository method overrides the configured consistency for that method.

## 8. Tests

```bash
./mvnw test
```

* `SpringEnvironmentDriverConfigSupplierTests` checks YAML → driver options (lists, profiles, precedence, re-reading).
  No database needed.
* `AstraSchemaCreatorTests` checks that the SAI DDL renders as `CREATE CUSTOM INDEX … USING 'StorageAttachedIndex'`.
  No database needed.
* `BookRepositoryAstraTests` runs repository queries, updates, deletes and LWTs against your Astra database. It runs
  only when `ASTRA_DB_APPLICATION_TOKEN` and `ASTRA_DB_SECURE_BUNDLE_PATH` are set. It uses its own random
  author/genre values and cleans up after itself.

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| `ASTRA_DB_APPLICATION_TOKEN is not set` / `Secure connect bundle not found` | Export the variables in the same shell; the bundle path must be absolute and readable |
| `Keyspace 'x' does not exist` | Create the keyspace in Astra; CQL `CREATE KEYSPACE` is not allowed |
| `Unauthorized … CREATE` on startup | The token's role can't create tables/indexes. Use a more privileged token, or create the schema yourself and set `spring.cassandra.schema-action=none` |
| Timeouts right after a database resumes from hibernation | Re-run; or raise `datastax-java-driver.basic.request.timeout` |
| A write after an `IF …` operation on the same row doesn't stick | See *Don't mix LWT and non-LWT writes* in section 6 |

## Dataset

[mouraleonardo/books_dataset](https://github.com/mouraleonardo/books_dataset) (GPL-2.0) is downloaded at runtime and is
not copied into this repository. To work offline, download it once and point `books.dataset.location` at the file:

```bash
curl -L -o /tmp/books_dataset.json \
  https://raw.githubusercontent.com/mouraleonardo/books_dataset/development/books_dataset.json
./mvnw spring-boot:run -Dspring-boot.run.arguments="--books.dataset.location=file:/tmp/books_dataset.json"
```
