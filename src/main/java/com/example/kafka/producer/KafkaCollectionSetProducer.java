package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.opennms.features.kafka.producer.model.CollectionSetProtos;

import java.util.Properties;

public class KafkaCollectionSetProducer {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final Properties producerProps;

    public KafkaCollectionSetProducer(Properties producerProps, String topic) {
        this.topic = topic;
        this.producerProps = producerProps;

        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer<>(producerProps);
    }

    public void send(CollectionSetProtos.CollectionSet cs) {
        producer.send(
                new ProducerRecord<>(topic, cs.toByteArray()),
                (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                }
        );
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
