package com.example.kafka.producer;

import org.opennms.features.kafka.producer.model.CollectionSetProtos;

import java.util.ArrayList;
import java.util.Random;

public class RandomCollectionSetGenerator {

    private static final Random RAND = new Random();

    public static CollectionSetProtos.CollectionSet generate(long maxNumerics, long maxStrings) {
        long now = System.currentTimeMillis();
        int nodeId = RAND.nextInt(1000);
        String nodeLabel = "node-" + RAND.nextInt(1000);
        String foreignId = "fs-" + RAND.nextInt(10000);
        long randoNumerics = RAND.nextLong(maxNumerics) + 1;
        long randoStrings = RAND.nextLong(maxStrings);
        ArrayList<CollectionSetProtos.NumericAttribute> numerics = new ArrayList<>();
        ArrayList<CollectionSetProtos.StringAttribute> strings = new ArrayList<>();

        CollectionSetProtos.NumericAttribute thisNumeric;
        for (int i = 0; i < randoNumerics; i++) {
            thisNumeric = CollectionSetProtos.NumericAttribute.newBuilder()
                            .setName("gaugeValue-"+i)
                            .setValue(RAND.nextDouble() * 100)
                            .setType(CollectionSetProtos.NumericAttribute.Type.GAUGE)
                            .build();
            numerics.add(thisNumeric);
        }
        CollectionSetProtos.StringAttribute thisString;
        for (int i = 0; i < randoStrings; i++) {
            thisString = CollectionSetProtos.StringAttribute.newBuilder()
                            .setName("metricLabel-"+i )
                            .setValue("aStringValue-" + RAND.nextLong(maxStrings))
                            .build();
            strings.add(thisString);
        }

        CollectionSetProtos.CollectionSetResource resource =
                CollectionSetProtos.CollectionSetResource.newBuilder()
                        .setNode(
                                CollectionSetProtos.NodeLevelResource.newBuilder()
                                        .setNodeId(nodeId)
                                        .setNodeLabel(nodeLabel)
                                        .setForeignSource("test")
                                        .setForeignId(foreignId)
                                        .build()
                        )
                        .addAllNumeric(numerics)
                        .addAllString(strings)
                        .build();

        return CollectionSetProtos.CollectionSet.newBuilder()
                .setTimestamp(now)
                .addResource(resource)
                .build();
    }
}
