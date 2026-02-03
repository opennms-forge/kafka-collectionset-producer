package com.example.kafka.producer;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CollectionSetProducer {

    private static final MetricRegistry metrics = new MetricRegistry();
    public static final Timer messageMetrics = metrics.timer("messageMetrics");

    public static void main(String[] args) throws IOException, InterruptedException {

        ConsoleReporter reporter;
        try {
            reporter = ConsoleReporter.forRegistry(metrics).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        reporter.start(5,10, TimeUnit.SECONDS);
        Options options = new Options();

        options.addRequiredOption(null, "config", true,
                "Path to Kafka producer properties file (Required!)");

        options.addOption(Option.builder()
                .longOpt("bootstrap-servers")
                .hasArg()
                .desc("Override Kafka bootstrap servers in config (Default: localhost:9092")
                .build());

        options.addOption(Option.builder()
                .longOpt("topic")
                .hasArg()
                .desc("Target Kafka topic name (Default: metrics)")
                .build());

        options.addOption(Option.builder()
                .longOpt("producers")
                .hasArg()
                .desc("Number of producer threads (default: 1)")
                .build());

        options.addOption(Option.builder()
                .longOpt("messages")
                .hasArg()
                .desc("Limit number of messages per producer (Default: 0 - no limit)")
                .build());

        options.addOption(Option.builder()
                .longOpt("delayms")
                .hasArg()
                .desc("Delay (in milliseconds) between sending each message (Default: 100)")
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("kafka-collectionset-producer", options);
            return;
        }
        Properties props = KafkaConfigLoader.load(cmd.getOptionValue("config"));

        // bootstrap server override
        if (cmd.hasOption("bootstrap-servers")) {
            props.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    cmd.getOptionValue("bootstrap-servers")
            );
        }

        String topic = cmd.getOptionValue("topic", "metrics");
        int producers = Integer.parseInt(cmd.getOptionValue("producers", "1"));
        int messages = Integer.parseInt(cmd.getOptionValue("messages", "0"));
        int delayMs = Integer.parseInt(cmd.getOptionValue("delayms", "100"));

        System.out.printf(
                " bootstrap=%s%n topic=%s%n producer_threads=%d%n messages=%d%n",
                props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), topic, producers, messages
        );

        ExecutorService executor = Executors.newFixedThreadPool(producers);

        KafkaCollectionSetProducer producer =
                new KafkaCollectionSetProducer(props, topic);

        for (int i = 0; i < producers; i++) {
            executor.submit(new ProducerWorker(producer, messages, delayMs));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            executor.shutdownNow();
        }));

        // show producer metrics
        reporter.report();
    }
}
