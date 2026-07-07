package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * This service's side of the account-deletion saga. It consumes {@code content-commands} (the
 * command microservice-security's outbox publishes), purges the leaver's collections wholesale, and
 * confirms on {@code usercollections-events} — the third participant security waits for. The purge
 * is idempotent, so at-least-once delivery needs no extra dedup. The correlation id rides the Kafka
 * header, in and out, so the async hop keeps the trace of the request that started the deletion.
 */
public class PurgeCommandsConsumer {

    static final String COMMANDS_TOPIC = "content-commands";
    static final String EVENTS_TOPIC = "usercollections-events";
    static final String CID_HEADER = "X-Correlation-Id";

    private static final Logger LOG = LoggerFactory.getLogger(PurgeCommandsConsumer.class);

    private final PurgeUserItems purgeUserItems;
    private final ObjectMapper mapper;

    public PurgeCommandsConsumer(PurgeUserItems purgeUserItems, ObjectMapper mapper) {
        this.purgeUserItems = purgeUserItems;
        this.mapper = mapper;
    }

    /**
     * Handle one command payload: purge the user and return the confirmation to publish, or empty
     * for a command that is not ours (unknown type / malformed). Pure and broker-free, so the saga
     * scenario can drive it directly.
     */
    public Optional<String> handle(String commandPayload) {
        JsonNode command;
        try {
            command = mapper.readTree(commandPayload);
        } catch (Exception malformed) {
            LOG.warn("dropping malformed command: " + commandPayload);
            return Optional.empty();
        }
        if (!"PURGE_USER_CONTENT".equals(command.path("type").asText())) {
            return Optional.empty();
        }
        String email = command.path("email").asText();
        int removed = purgeUserItems.execute(email);
        String sagaId = command.path("sagaId").asText();
        LOG.info("purged " + removed + " collection refs of " + email + " (saga " + sagaId + ")");
        try {
            return Optional.of(mapper.writeValueAsString(mapper.createObjectNode()
                    .put("type", "USER_CONTENT_PURGED")
                    .put("sagaId", sagaId)
                    .put("email", email)));
        } catch (Exception impossible) {
            throw new IllegalStateException("could not build confirmation", impossible);
        }
    }

    /**
     * The real Kafka loop: poll commands, handle each, publish the confirmation forwarding the cid,
     * then commit. Runs on a daemon virtual thread started from {@link Main} when a broker is
     * configured; absent a broker (dev, tests) it simply never runs.
     */
    public void run(String bootstrapServers) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrapServers));
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrapServers))) {
            consumer.subscribe(List.of(COMMANDS_TOPIC));
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    String cid = header(record, CID_HEADER);
                    if (cid != null) {
                        MDC.put("cid", cid);   // continue the trace the deletion request started
                    }
                    try {
                        handle(record.value()).ifPresent(confirmation -> {
                            ProducerRecord<String, String> out =
                                    new ProducerRecord<>(EVENTS_TOPIC, record.key(), confirmation);
                            if (cid != null) {
                                out.headers().add(CID_HEADER, cid.getBytes(StandardCharsets.UTF_8));
                            }
                            producer.send(out);
                        });
                    } finally {
                        MDC.remove("cid");
                    }
                }
                consumer.commitSync();
            }
        } catch (Exception broken) {
            LOG.warn("purge consumer stopped: " + broken.getMessage());
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "user-collections");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
