package com.jrobertgardzinski.collections.infrastructure;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The build-time guard on the retired key: <strong>the owner id is the key of a row, the e-mail
 * address is not.</strong> The cutover moved every read, every ownership check and the whole
 * closure onto the id; what it cannot do is stop the next honestly-written SELECT from matching
 * saved items by address again. A query like that looks harmless and quietly brings back the four
 * defects the id was adopted to end: a member who renames becomes a stranger to their own content,
 * their closure marks nothing while confirming an erasure, and whoever registers the freed address
 * next inherits what was left under it.
 *
 * <p><strong>Why this shape and not an ArchUnit rule.</strong> The same reason
 * {@code MemeReadFilterTest} gives: the rule is about the text of a SQL string, and a
 * bytecode-level rule cannot see inside a string constant. So it is enforced over the literals.
 *
 * <p>Only STRING LITERALS are scanned for the predicate, never prose — this javadoc names the
 * forbidden column several times, and a guard that a comment can trip is a guard that gets deleted.
 * The retired-machinery rule below is the opposite: it reads the whole file, identifiers included,
 * because {@code Rekey} and {@code EMAIL_CHANGED} must not come back as a class or a topic name
 * either.
 *
 * <p>What is deliberately NOT forbidden, each for a recorded reason:
 * <ul>
 *   <li>nothing here writes an address any more — collections dropped the column outright, which
 *       is why this rule needs no exemption and the schema test below is the one that holds the
 *       line.</li>
 *   <li>{@code user_id} is the whole key, including inside {@code uq_collection_item}, which 1e
 *       rebuilt on it.</li>
 * </ul>
 *
 * <p>The last test is the counterweight: the scan MUST have seen SQL at all, so the rules above
 * cannot pass in a world where the queries moved somewhere this test does not look.
 */
@Epic("Identity")
@Feature("The id is the only key")
@Story("No address-keyed SQL")
class RetiredAddressKeyTest {

    /** The whole service from this module: every module's main sources, not just this one's. */
    private static final Path SERVICE = Path.of("..");

    private static final Path SCHEMA = Path.of("src/main/resources/db/migration/V1__schema.sql");

    /**
     * {@code WHERE user_email =}, and any other address-shaped column a later row might be looked
     * up by. Collections kept no address at all — 1e dropped the column instead of demoting it to
     * an attribute, which is why this service has no {@code author}-style word to spare.
     */
    private static final Pattern ADDRESS_PREDICATE = Pattern.compile(
            "\\b(WHERE|AND|OR)\\s+(user_email|email|user_address)\\s*=", Pattern.CASE_INSENSITIVE);

    /** The machinery 1e deleted: the rekey use case and adapters, and the fact that drove them. */
    private static final Pattern RETIRED_MACHINERY = Pattern.compile("Rekey|EMAIL_CHANGED|user_email");

    /** A Java string literal, escapes included, so a SQL fragment split over "+" is still seen. */
    private static final Pattern STRING_LITERAL = Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"");

    /** Enough of a query to prove the scan is looking at SQL and not at an empty directory. */
    private static final Pattern ANY_READ = Pattern.compile("\\b(FROM|JOIN)\\s+\\w", Pattern.CASE_INSENSITIVE);

    @Test
    @DisplayName("no query matches a row by the address")
    void every_query_is_keyed_by_the_id() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Path source : mainSources()) {
            for (String sql : stringLiteralsIn(source)) {
                if (ADDRESS_PREDICATE.matcher(sql).find()) {
                    offenders.add(source.getFileName() + ": " + sql);
                }
            }
        }
        assertTrue(offenders.isEmpty(),
                "these queries match saved items by e-mail address, which the cutover retired as a "
                        + "key: an address moves, so the query answers for the wrong member — or for "
                        + "whoever registered it next. Match on user_id instead:\n  "
                        + String.join("\n  ", offenders));
    }

    @Test
    @DisplayName("no main source brings the rekey machinery or its fact back")
    void the_retired_machinery_stays_deleted() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Path source : mainSources()) {
            Matcher matcher = RETIRED_MACHINERY.matcher(Files.readString(source));
            if (matcher.find()) {
                offenders.add(source.getFileName() + ": " + matcher.group());
            }
        }
        assertTrue(offenders.isEmpty(),
                "1e deleted the rekey use case, its adapters and the EMAIL_CHANGED consumer, because "
                        + "content keyed by an id has nothing to re-key when an address changes. "
                        + "Something here names them again:\n  " + String.join("\n  ", offenders));
    }

    @Test
    @DisplayName("the schema keeps no address-shaped key")
    void the_schema_is_keyed_by_the_id() throws IOException {
        String schema = Files.readString(SCHEMA);
        assertFalse(schema.contains("user_email"),
                "the user_email column went with the cutover; the id column is the key");
        assertFalse(Pattern.compile("(?i)create\\s+index\\s+\\S+\\s+on\\s+\\w+\\s*\\(\\s*user_email\\s*\\)")
                        .matcher(schema).find(),
                "an index on user_email alone is a key in all but name: it exists to look rows up by "
                        + "address. The key is user_id.");
        assertTrue(schema.contains("idx_collection_items_user"),
                "the index the id is looked up by is gone, so the rule above is passing for the "
                        + "wrong reason: either the key moved, or the table did");
    }

    @Test
    @DisplayName("the exemption-free rules are earned: the scan really does see SQL")
    void the_scan_sees_sql_at_all() throws IOException {
        List<String> reads = new ArrayList<>();
        for (Path source : mainSources()) {
            stringLiteralsIn(source).stream().filter(sql -> ANY_READ.matcher(sql).find()).forEach(reads::add);
        }
        assertFalse(reads.isEmpty(),
                "not one SQL read was found in this service's main sources, so the rules above prove "
                        + "nothing: the queries moved somewhere this test does not look");
    }

    private static List<Path> mainSources() throws IOException {
        try (Stream<Path> tree = Files.walk(SERVICE)) {
            return tree.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> path.toString().contains("src/main/java"))
                    .toList();
        }
    }

    private static List<String> stringLiteralsIn(Path source) throws IOException {
        List<String> literals = new ArrayList<>();
        Matcher matcher = STRING_LITERAL.matcher(Files.readString(source));
        while (matcher.find()) {
            literals.add(matcher.group(1));
        }
        return literals;
    }
}
