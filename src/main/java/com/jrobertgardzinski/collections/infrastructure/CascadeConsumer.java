package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * The DELETION CASCADE: when a meme dies, the references to it die with it, and so do the
 * references to the comments that died with the meme. Two topics, two event types, one purpose —
 * no collection may keep pointing at something the portal no longer has.
 *
 * <ul>
 *   <li>{@code memes-events} / {@code MEME_DELETED} → drop every ref {@code (meme, memeId)}</li>
 *   <li>{@code comments-events} / {@code COMMENTS_DELETED} → drop every ref
 *       {@code (comment, id)} for each id the event names</li>
 * </ul>
 *
 * <h2>Why this is a SEPARATE loop and not two more branches in {@link PurgeCommandsConsumer}</h2>
 *
 * The two look alike — poll, parse, purge, commit — and are opposites where it counts. The saga
 * consumer is a PROMISE: an orchestrator is blocked on its confirmation, a lost purge is a broken
 * GDPR commitment, so it retries forever without committing, rewinds failed batches, probes the
 * broker for honesty and drives {@code /health} and {@code /alive}. This one is a CHOREOGRAPHY:
 * nobody waits for it, nothing confirms, nothing compensates, and the worst outcome of total
 * failure is a row pointing at a meme that no longer exists — which the UI already renders as
 * exactly that (see collections-ui) instead of pretending it is live.
 *
 * <p>Folding the cascade into the saga loop would have cost both of them:
 * <ul>
 *   <li><b>Shared fate.</b> One subscription means one set of offsets and one failure path. A
 *       storm of MEME_DELETED events, or a cascade purge stuck behind a lock, would freeze the
 *       saga's readiness marker and turn {@code /health} 503 — advertising "this instance cannot
 *       do its saga share" because a best-effort cleanup was slow.</li>
 *   <li><b>Wrong retry policy for whichever loses.</b> The saga's endless retry applied to a
 *       cascade event would wedge a partition forever over a dead row; the cascade's give-up
 *       applied to a purge command would silently drop a GDPR obligation.</li>
 *   <li><b>The probe would stop meaning anything.</b> {@code PurgeCommandsConsumer} probes ONE
 *       named topic on purpose, and {@link Main#ALIVE_STALL_FLOOR} is arithmetic over that one
 *       loop's clocks. A second topic and a second class of work would make both the probe and
 *       the floor describe something other than what they measure.</li>
 * </ul>
 *
 * So: its own thread, its own consumer group ({@link #GROUP_ID}), its own — far shorter — set of
 * rules, and deliberately NOT watched by either probe. A cascade that has stopped is a cleanup
 * debt, not an unready instance, and a readiness probe that goes red for cleanup debt teaches
 * operators to ignore it.
 *
 * <h2>Why ONE loop for the two cascade topics</h2>
 *
 * Two topics, but identical mechanics and identical (best-effort) semantics, so they share a
 * thread and a group; {@code ConsumerRecord.topic()} is all the routing they need. A second thread
 * would need a second group id (two members of one group subscribing to different topics fight
 * over the assignment), doubling the moving parts to isolate two paths that cannot harm each
 * other anyway: neither retries forever, so neither can hold the other up for longer than
 * {@link #MAX_ATTEMPTS} short backoffs.
 *
 * <h2>The rules, in full</h2>
 *
 * <ul>
 *   <li><b>Foreign types are silence.</b> Both topics are shared. {@code comments-events} also
 *       carries the saga's {@code USER_CONTENT_PURGED} confirmations and ordinary comment
 *       lifecycle events; {@code memes-events} carries the rest of a meme's life. Anything that
 *       is not ours is skipped without a log line — a WARN per foreign event would drown the real
 *       ones.</li>
 *   <li><b>Poison pills are dropped, never echoed.</b> Unparseable JSON, or an event whose id is
 *       missing or is not an id at all, cannot be fixed by any retry, so it is committed away
 *       with a WARN that reports the SHAPE of the problem and never the payload. Ids are not PII,
 *       and the {@code memeId} is logged deliberately — it is the one handle an operator has on
 *       what went past.</li>
 *   <li><b>A failed purge is retried a few times, then abandoned.</b> A database hiccup should
 *       not cost a cleanup, so a failing record is re-tried in place ({@link #MAX_ATTEMPTS}
 *       attempts, short backoff, no rewind needed — the batch is already in memory and the purge
 *       is idempotent). After that the offset commits and the rows stay: the accepted worst case,
 *       and strictly better than wedging the partition so that no LATER deletion is cascaded
 *       either.</li>
 *   <li><b>The correlation id rides through.</b> Same header as everywhere else in the portal, so
 *       the cascade's log lines join the trace of the request that deleted the meme.</li>
 * </ul>
 */
public class CascadeConsumer {

    static final String MEMES_TOPIC = "memes-events";
    static final String COMMENTS_TOPIC = "comments-events";

    /**
     * Its OWN group — the single most important line in this class. Separate offsets, separate
     * lifecycle, separate lag: the cascade can fall behind, be reset, or be switched off entirely
     * without the saga consumer noticing.
     */
    static final String GROUP_ID = "user-collections-cascade";

    static final String CID_HEADER = "X-Correlation-Id";

    /** The two item types this service's refs use for the two cascading sources. */
    static final String MEME_ITEM_TYPE = "meme";
    static final String COMMENT_ITEM_TYPE = "comment";

    static final Duration POLL_EVERY = Duration.ofSeconds(1);

    /**
     * The consumer's blocking clock, EXPLICIT for the same reason it is in
     * {@link PurgeCommandsConsumer}: {@code commitSync()} waits up to
     * {@code default.api.timeout.ms} on an unresponsive broker and Kafka leaves that at 60s.
     * Nothing here drives a liveness probe, but an iteration that can block for a minute is still
     * a minute this thread is not cascading anything.
     */
    static final Duration API_TIMEOUT = Duration.ofSeconds(20);
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    /**
     * How many events one poll may hand back. Unlike the saga loop this one pays NO broker round
     * trip per record (there is no confirmation to publish), so the batch costs N store calls and
     * nothing else — but the ceiling matters for the same reason: after an outage the first poll
     * drains the backlog, and a batch that takes longer than {@code max.poll.interval.ms} gets
     * the member kicked out of the group mid-batch. 50 keeps a drained backlog to portions this
     * loop finishes and commits.
     */
    static final int MAX_POLL_RECORDS = 50;

    /**
     * How many times one record's purge is attempted before the cascade gives up on it and leaves
     * the rows behind. Three, with {@link #RETRY_BACKOFF} between them: enough to ride out a
     * connection-pool blip or a lock held by somebody else's transaction, far too few to turn a
     * real database outage into a wedged partition. The bound is what keeps a failing batch's
     * cost predictable — at most {@code MAX_ATTEMPTS - 1} sleeps per poll, not per record, because
     * the retries are batched (see {@link #handleBatch}).
     */
    static final int MAX_ATTEMPTS = 3;
    static final Duration RETRY_BACKOFF = Duration.ofSeconds(1);

    /**
     * A canonical UUID is 36 characters. Both event contracts spell their ids as uuids, so
     * anything else on the wire is a mis-produced event rather than an id this service has simply
     * never seen — and {@link UUID#fromString} alone would accept lax forms like {@code 1-1-1-1-1}.
     * Note what this does NOT do: it does not judge the ids already stored. {@link
     * com.jrobertgardzinski.collections.domain.ItemRef} is opaque by design; this check belongs to
     * the EVENT contract, at the boundary, which is exactly where a boundary check belongs.
     */
    private static final int UUID_LENGTH = 36;

    private static final Logger LOG = LoggerFactory.getLogger(CascadeConsumer.class);

    private final PurgeDeletedItem purgeDeletedItem;
    private final ObjectMapper mapper;
    private final long retryBackoffMillis;

    public CascadeConsumer(PurgeDeletedItem purgeDeletedItem, ObjectMapper mapper) {
        this(purgeDeletedItem, mapper, RETRY_BACKOFF.toMillis());
    }

    /** Test seam: the loop-under-test retries in milliseconds instead of sleeping out seconds. */
    CascadeConsumer(PurgeDeletedItem purgeDeletedItem, ObjectMapper mapper,
                    long retryBackoffMillis) {
        this.purgeDeletedItem = purgeDeletedItem;
        this.mapper = mapper;
        this.retryBackoffMillis = retryBackoffMillis;
    }

    /**
     * Handle one event: purge whatever it says is gone and return how many refs went. Pure and
     * broker-free (the topic is passed in rather than read off a record), so the tests drive the
     * whole contract without a Kafka in sight.
     *
     * <p>Returns 0 for everything that is not ours and for every poison pill; the only thing that
     * ever escapes is a genuine store failure, which the loop is what decides how to treat.
     */
    public int handle(String topic, String payload) {
        JsonNode event;
        try {
            event = mapper.readTree(payload);
        } catch (Exception malformed) {
            // never the payload itself — the portal's rule everywhere, and it costs nothing to
            // keep here: the size and the topic are enough to find the producer
            LOG.warn("dropping a malformed {} event ({} chars, not valid JSON)",
                    topic, payload == null ? 0 : payload.length());
            return 0;
        }
        String type = event.path("type").asText();
        if (MEMES_TOPIC.equals(topic) && "MEME_DELETED".equals(type)) {
            return onMemeDeleted(event);
        }
        if (COMMENTS_TOPIC.equals(topic) && "COMMENTS_DELETED".equals(type)) {
            return onCommentsDeleted(event);
        }
        // both topics are shared with other conversations (comments-events carries the saga's own
        // USER_CONTENT_PURGED confirmations) — not ours, not our business, and not worth a line
        return 0;
    }

    /** {@code MEME_DELETED}: every ref to the meme itself. */
    private int onMemeDeleted(JsonNode event) {
        String memeId = event.path("memeId").asText();
        if (isNotAnId(memeId)) {
            // a deletion that names nothing: no retry can invent the id, so it is committed away
            LOG.warn("dropping a MEME_DELETED whose memeId is missing or is not an id");
            return 0;
        }
        int removed = purgeDeletedItem.execute(MEME_ITEM_TYPE, List.of(memeId));
        LOG.info("cascade removed {} collection refs to deleted meme {}", removed, memeId);
        return removed;
    }

    /**
     * {@code COMMENTS_DELETED}: every ref to the comments that went down with the meme. The
     * {@code memeId} is the event's key and its handle in the log — a COMMENTS_DELETED without one
     * is off-contract and dropped, because a cascade nobody can trace back to a meme is a cascade
     * nobody can audit.
     */
    private int onCommentsDeleted(JsonNode event) {
        String memeId = event.path("memeId").asText();
        if (isNotAnId(memeId)) {
            LOG.warn("dropping a COMMENTS_DELETED whose memeId is missing or is not an id");
            return 0;
        }
        List<String> commentIds = new ArrayList<>();
        int unusable = 0;
        for (JsonNode id : event.path("commentIds")) {
            String commentId = id.asText();
            if (isNotAnId(commentId)) {
                unusable++;
            } else {
                commentIds.add(commentId);
            }
        }
        if (unusable > 0) {
            // partial poison: purge what CAN be purged rather than abandoning the whole event —
            // best-effort means every ref we can honestly account for still goes. The count, not
            // the ids, because a garbage id is untrusted content
            LOG.warn("{} of the ids in a COMMENTS_DELETED for meme {} are not ids and were"
                    + " skipped", unusable, memeId);
        }
        int removed = purgeDeletedItem.execute(COMMENT_ITEM_TYPE, commentIds);
        LOG.info("cascade removed {} collection refs to {} deleted comments of meme {}",
                removed, commentIds.size(), memeId);
        return removed;
    }

    /** The boundary's id check — see {@link #UUID_LENGTH} for why the contract, not the domain. */
    private static boolean isNotAnId(String value) {
        if (value == null || value.length() != UUID_LENGTH) {
            return true;
        }
        try {
            UUID.fromString(value);
            return false;
        } catch (IllegalArgumentException notAnId) {
            return true;
        }
    }

    /**
     * The real Kafka loop, on a daemon virtual thread started from {@link Main} when a broker is
     * configured; absent a broker (dev, tests) it simply never runs.
     */
    public void run(String bootstrapServers) {
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps(bootstrapServers))) {
            run(consumer);
        } catch (Exception fatal) {
            // client construction or close: a config error no retry can fix, but one that must
            // not vanish silently with the daemon thread
            LOG.error("cascade consumer stopped for good", fatal);
        }
    }

    /**
     * The loop against the {@link Consumer} interface — the test seam, driven by kafka-clients'
     * own MockConsumer while production passes the real client from {@link #run(String)}.
     *
     * <p>Notice what is NOT here, compared with {@link PurgeCommandsConsumer#run}: no producer, no
     * rewind, no broker probe, no health markers, no endless retry. Those exist there to keep a
     * saga's promise. Here the entire failure policy is "try a few times, then leave a dead row
     * and keep going", so a cycle that fails simply does not commit and the next poll carries on;
     * a restart replays from the last committed offset and the idempotent purge absorbs it.
     */
    void run(Consumer<String, String> consumer) {
        consumer.subscribe(List.of(MEMES_TOPIC, COMMENTS_TOPIC));
        LOG.info("cascade consumer listening on {} and {} as group {}",
                MEMES_TOPIC, COMMENTS_TOPIC, GROUP_ID);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(POLL_EVERY);
                if (!handleBatch(records)) {
                    return;   // interrupted mid-backoff; the flag is already restored
                }
                consumer.commitSync();
            } catch (InterruptException stopping) {
                return;   // the JVM is going down; Kafka re-set the interrupt flag already
            } catch (Exception cycleFailed) {
                // the broker end of things: a failed poll or commit. Nothing to rewind — the
                // purges already ran and are idempotent, so the only consequence of an
                // uncommitted batch is that a restart replays it for free
                LOG.warn("cascade cycle failed; the offsets stay where they were", cycleFailed);
                if (!backedOff(retryBackoffMillis)) {
                    return;
                }
            }
        }
    }

    /**
     * Handle one poll's records, retrying only the ones that failed. Retrying the BATCH rather
     * than each record in turn is what keeps a failing poll's cost bounded: a database that is
     * down fails all 50 records, and a per-record retry would pay
     * {@code 50 x (MAX_ATTEMPTS - 1)} backoffs in one iteration — enough to walk past
     * {@code max.poll.interval.ms} and get the member evicted mid-batch. Batched, the same outage
     * costs {@code MAX_ATTEMPTS - 1} backoffs total.
     *
     * <p>No rewind is needed for the retry: the records are already in memory, and re-handling one
     * that already succeeded would be harmless anyway (the purge matches nothing the second time).
     * Returns false only when the stop interrupt cut a backoff short.
     */
    private boolean handleBatch(ConsumerRecords<String, String> records) {
        List<ConsumerRecord<String, String>> pending = new ArrayList<>();
        records.forEach(pending::add);
        for (int attempt = 1; !pending.isEmpty(); attempt++) {
            List<ConsumerRecord<String, String>> failed = new ArrayList<>();
            for (ConsumerRecord<String, String> record : pending) {
                if (!handledRecord(record, attempt)) {
                    failed.add(record);
                }
            }
            pending = failed;
            if (pending.isEmpty()) {
                return true;
            }
            if (attempt >= MAX_ATTEMPTS) {
                // the accepted worst case, said out loud: the references survive their targets
                // until something else removes them. Better than a wedged partition, which would
                // cost every LATER deletion its cascade too
                LOG.warn("cascade gave up on {} event(s) after {} attempts — the references they"
                        + " name stay behind as dead rows", pending.size(), MAX_ATTEMPTS);
                return true;
            }
            if (!backedOff(retryBackoffMillis)) {
                return false;
            }
        }
        return true;
    }

    /**
     * One record: correlation id into the MDC, then the purge. True when it is done with (handled,
     * ignored or dropped as a poison pill), false when the store failed and a retry is worth it.
     */
    private boolean handledRecord(ConsumerRecord<String, String> record, int attempt) {
        String cid = header(record, CID_HEADER);
        if (cid != null) {
            MDC.put("cid", cid);   // continue the trace the deletion request started
        }
        try {
            handle(record.topic(), record.value());
            return true;
        } catch (Exception purgeFailed) {
            // the store, not the event — the event has already been parsed and accepted. The
            // topic and the attempt say what to look at; the payload still stays out
            LOG.warn("cascade purge for a {} event failed on attempt {} of {}",
                    record.topic(), attempt, MAX_ATTEMPTS, purgeFailed);
            return false;
        } finally {
            MDC.remove("cid");
        }
    }

    /** Sleep the backoff; false when the stop interrupt cut it short (flag already restored). */
    private static boolean backedOff(long backoffMillis) {
        try {
            Thread.sleep(backoffMillis);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Package-private so the test can pin the group and the clocks. */
    static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "false");
        // earliest, and that is a FEATURE on the first deployment: this service has been saving
        // refs for longer than it has been cascading, so the first run walks the retained history
        // of both topics and clears the dead rows that accumulated before the cascade existed.
        // It costs nothing to repeat — every purge here is idempotent
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("default.api.timeout.ms", String.valueOf(API_TIMEOUT.toMillis()));
        props.put("request.timeout.ms", String.valueOf(REQUEST_TIMEOUT.toMillis()));
        props.put("max.poll.records", String.valueOf(MAX_POLL_RECORDS));
        return props;
    }
}
