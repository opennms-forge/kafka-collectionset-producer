package com.example.kafka.producer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.clients.producer.*;
import org.opennms.features.kafka.producer.model.CollectionSetProtos;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;

public class KafkaCollectionSetProducer {

    // Matches the printer used by the OpenNMS opennms-kafka-producer feature (KafkaPersister).
    // NumericAttribute.type must always print: GAUGE is the zero enum value, which the
    // printer would otherwise omit, leaving gauges without a type key while counters keep theirs.
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .includingDefaultValueFields(Collections.singleton(
                    CollectionSetProtos.NumericAttribute.getDescriptor()
                            .findFieldByNumber(CollectionSetProtos.NumericAttribute.TYPE_FIELD_NUMBER)));

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final boolean useJson;
    private final Properties producerProps;

    public KafkaCollectionSetProducer(Properties producerProps, String topic) {
        this(producerProps, topic, "protobuf");
    }

    public KafkaCollectionSetProducer(Properties producerProps, String topic, String format) {
        this.topic = topic;
        this.producerProps = producerProps;
        this.useJson = "json".equalsIgnoreCase(format);

        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer<>(producerProps);
    }

    public void send(CollectionSetProtos.CollectionSet cs) {
        byte[] payload = serialize(cs);
        if (payload == null) {
            return;
        }
        producer.send(
                new ProducerRecord<>(topic, payload),
                (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                }
        );
    }

    /**
     * Serialize the CollectionSet with the configured output format:
     * JSON when --format json was given, protobuf otherwise.
     * Returns null if the CollectionSet could not be serialized.
     */
    byte[] serialize(CollectionSetProtos.CollectionSet cs) {
        if (useJson) {
            try {
                return JSON_PRINTER.print(cs).getBytes(StandardCharsets.UTF_8);
            } catch (InvalidProtocolBufferException e) {
                System.err.println("Failed to serialize CollectionSet to JSON, it will not be sent: " + e.getMessage());
                return null;
            }
        }
        return cs.toByteArray();
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
