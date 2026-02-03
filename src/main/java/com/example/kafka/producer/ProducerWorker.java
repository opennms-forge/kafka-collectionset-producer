package com.example.kafka.producer;

import com.codahale.metrics.Timer;

public class ProducerWorker implements Runnable {

    private final KafkaCollectionSetProducer producer;
    private final int messages;
    private final long delayMs;
    private final long maxNumerics;
    private final long maxStrings;

    public ProducerWorker(KafkaCollectionSetProducer producer,
                          int messages,
                          long delayMs,
                          long maxNumerics,
                          long maxStrings) {
        this.producer = producer;
        this.messages = messages;
        this.delayMs = delayMs;
        this.maxNumerics = maxNumerics;
        this.maxStrings = maxStrings;
    }

    @Override
    public void run() {
        try {
            if (messages > 0) {
                System.out.println("message > 0");
                for (int i = 0; i < messages; i++) {
                    try (Timer.Context ctx = CollectionSetProducer.messageMetrics.time()) {
                        producer.send(RandomCollectionSetGenerator.generate(maxNumerics, maxStrings));
                    }
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                }
            } else {
                System.out.println("messages forever");
                while (true) {
                    try (Timer.Context ctx = CollectionSetProducer.messageMetrics.time()) {
                        producer.send(RandomCollectionSetGenerator.generate(maxNumerics, maxStrings));
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
