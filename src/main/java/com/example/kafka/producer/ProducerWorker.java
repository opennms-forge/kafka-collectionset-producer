package com.example.kafka.producer;

import com.codahale.metrics.Timer;

public class ProducerWorker implements Runnable {

    private final KafkaCollectionSetProducer producer;
    private final int messages;
    private final long delayMs;

    public ProducerWorker(KafkaCollectionSetProducer producer,
                          int messages,
                          long delayMs) {
        this.producer = producer;
        this.messages = messages;
        this.delayMs = delayMs;
    }

    @Override
    public void run() {
        try {
            if (messages > 0) {
                for (int i = 0; i < messages; i++) {
                    try (Timer.Context ctx = CollectionSetProducer.messageMetrics.time()) {
                        producer.send(RandomCollectionSetGenerator.generate());
                    }
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                }
            } else {
                while (true) {
                    try (Timer.Context ctx = CollectionSetProducer.messageMetrics.time()) {
                        producer.send(RandomCollectionSetGenerator.generate());
                    }
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
