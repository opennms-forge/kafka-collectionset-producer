package com.example.kafka.producer;

import org.opennms.features.kafka.producer.model.CollectionSetProtos;
import com.google.protobuf.Timestamp;

import java.util.Random;

public class RandomCollectionSetGenerator {

    private static final Random RAND = new Random();

    public static CollectionSetProtos.CollectionSet generate() {
        long now = System.currentTimeMillis();

        CollectionSetProtos.NumericAttribute cpu =
                CollectionSetProtos.NumericAttribute.newBuilder()
                        .setName("cpuLoad")
                        .setValue(RAND.nextDouble() * 100)
                        .setType(CollectionSetProtos.NumericAttribute.Type.GAUGE)
                        .build();
        CollectionSetProtos.StringAttribute cpuLabel =
                CollectionSetProtos.StringAttribute.newBuilder()
                        .setName("cpuLabel")
                        .setValue("cpuSlot-" + RAND.nextDouble() * 6)
                        .build();

        CollectionSetProtos.CollectionSetResource resource =
                CollectionSetProtos.CollectionSetResource.newBuilder()
                        .setNode(
                                CollectionSetProtos.NodeLevelResource.newBuilder()
                                        .setNodeId(RAND.nextInt(1000))
                                        .setNodeLabel("node-" + RAND.nextInt(100))
                                        .setForeignSource("test")
                                        .setForeignId("fs-" + RAND.nextInt(10000))
                                        .build()
                        )
                        .addNumeric(cpu)
                        .addString(cpuLabel)
                        .build();

        return CollectionSetProtos.CollectionSet.newBuilder()
                .setTimestamp(now)
                .addResource(resource)
                .build();
    }
}
