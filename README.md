# Kafka CollectionSet Producer

A reference implementation for producing OpenNMS metric CollectionSets, produced by the `opennms-kafka-producer` feature, to Apache Kafka.

## Installation

```
mvn clean install
```
... should produce a jar file in `target/`.

## Usage

Create a producer properties file to configure your environment.  An example properties file is available at `resources/producer.properties.example`. Options are described at [producer-configs](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)

Some properties can be set or overridden on the command-line:
```shell
$ java -jar target/opennms-kafka-collectionset-generator-1.0.0.jar --help
usage: kafka-collectionset-producer
    --bootstrap-servers <arg>   Override Kafka bootstrap servers in config
                                (Default: localhost:9092
    --config <arg>              Path to Kafka producer properties file
                                (Required!)
    --delayms <arg>             Delay (in milliseconds) between sending
                                each message (Default: 100)
    --messages <arg>            Limit number of messages per producer
                                (Default: 0 - no limit)
    --producers <arg>           Number of producer threads (default: 1)
    --topic <arg>               Target Kafka topic name (Default: metrics)
```

You can tailor the number of producer threads to your environment using the `--producers` option.
Specify the topic from which to consumer using the `--topic` option.

CollectionSets are generated according to [the CollectionSet proto](https://github.com/OpenNMS/opennms/blob/develop/features/kafka/producer/src/main/proto/collectionset.proto) containing random data, similar to:
```shell
timestamp: 1770145207771
resource {
  node {
    node_id: 124
    foreign_source: "test"
    foreign_id: "fs-3066"
    node_label: "node-85"
  }
  string {
    name: "cpuLabel"
    value: "cpuSlot-5.381154822927956"
  }
  numeric {
    name: "cpuLoad"
    value: 1.0593759044928097
  }
}
```
Each generated CollectionSet contains one numeric attribute and one string attribute. Real CollectionSets may contain many of each type.

These messsages can be consumed with the partner to this tool, the [Kafka CollectionSet Consumer](https://github.com/opennms-forge/kafka-collectionset-consumer)

## Instrumentation

While running, this tool will print timing metrics to the console, describing:

 * Number of messages produced
 * A histogram tracking the distribution of time in milliseconds to produce the message to Kafka
 * Per-second metric processing rate (1m/5m/15m)


## Example

```shell
$ cat /tmp/producer.properties
# https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html

bootstrap.servers=kafka:9094
acks=1

key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer

$ java -jar target/opennms-kafka-collectionset-generator-1.0.0.jar --config /tmp/producer.properties --producers 1 --topic metrics
 bootstrap=kafka:9094
 topic=metrics
 producer_threads=1
 messages=0
[main] INFO org.apache.kafka.common.config.AbstractConfig - ProducerConfig values:
        acks = -1
        batch.size = 16384
        bootstrap.servers = [kafka:9094]
        buffer.memory = 33554432
        client.dns.lookup = use_all_dns_ips
        client.id = producer-1
        compression.gzip.level = -1
        compression.lz4.level = 9
        compression.type = none
        compression.zstd.level = 3
        connections.max.idle.ms = 540000
        delivery.timeout.ms = 120000
        enable.idempotence = true
        enable.metrics.push = true
        interceptor.classes = []
        key.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer
        linger.ms = 5
        max.block.ms = 60000
        max.in.flight.requests.per.connection = 5
        max.request.size = 1048576
        metadata.max.age.ms = 300000
        metadata.max.idle.ms = 300000
        metadata.recovery.rebootstrap.trigger.ms = 300000
        metadata.recovery.strategy = rebootstrap
        metric.reporters = [org.apache.kafka.common.metrics.JmxReporter]
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        partitioner.adaptive.partitioning.enable = true
        partitioner.availability.timeout.ms = 0
        partitioner.class = null
        partitioner.ignore.keys = false
        receive.buffer.bytes = 32768
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retries = 2147483647
        retry.backoff.max.ms = 1000
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.connect.timeout.ms = null
        sasl.login.read.timeout.ms = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.login.retry.backoff.max.ms = 10000
        sasl.login.retry.backoff.ms = 100
        sasl.mechanism = GSSAPI
        sasl.oauthbearer.assertion.algorithm = RS256
        sasl.oauthbearer.assertion.claim.aud = null
        sasl.oauthbearer.assertion.claim.exp.seconds = 300
        sasl.oauthbearer.assertion.claim.iss = null
        sasl.oauthbearer.assertion.claim.jti.include = false
        sasl.oauthbearer.assertion.claim.nbf.seconds = 60
        sasl.oauthbearer.assertion.claim.sub = null
        sasl.oauthbearer.assertion.file = null
        sasl.oauthbearer.assertion.private.key.file = null
        sasl.oauthbearer.assertion.private.key.passphrase = null
        sasl.oauthbearer.assertion.template.file = null
        sasl.oauthbearer.client.credentials.client.id = null
        sasl.oauthbearer.client.credentials.client.secret = null
        sasl.oauthbearer.clock.skew.seconds = 30
        sasl.oauthbearer.expected.audience = null
        sasl.oauthbearer.expected.issuer = null
        sasl.oauthbearer.header.urlencode = false
        sasl.oauthbearer.jwks.endpoint.refresh.ms = 3600000
        sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms = 10000
        sasl.oauthbearer.jwks.endpoint.retry.backoff.ms = 100
        sasl.oauthbearer.jwks.endpoint.url = null
        sasl.oauthbearer.jwt.retriever.class = class org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever
        sasl.oauthbearer.jwt.validator.class = class org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator
        sasl.oauthbearer.scope = null
        sasl.oauthbearer.scope.claim.name = scope
        sasl.oauthbearer.sub.claim.name = sub
        sasl.oauthbearer.token.endpoint.url = null
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2, TLSv1.3]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.3
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS
        transaction.timeout.ms = 60000
        transaction.two.phase.commit.enable = false
        transactional.id = null
        value.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer

[main] INFO org.apache.kafka.common.telemetry.internals.KafkaMetricsCollector - initializing Kafka metrics collector
[main] INFO org.apache.kafka.clients.producer.KafkaProducer - [Producer clientId=producer-1] Instantiated an idempotent producer.
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka version: 4.1.1
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka commitId: be816b82d25370ce
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka startTimeMs: 1770147672433
2/3/26, 1:41:12 PM =============================================================

-- Timers ----------------------------------------------------------------------
messageMetrics
             count = 0
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.00 calls/second
    15-minute rate = 0.00 calls/second
               min = 0.00 milliseconds
               max = 0.00 milliseconds
              mean = 0.00 milliseconds
            stddev = 0.00 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 0.00 milliseconds
              98% <= 0.00 milliseconds
              99% <= 0.00 milliseconds
            99.9% <= 0.00 milliseconds


[kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.Metadata - [Producer clientId=producer-1] Cluster ID: AuWHI3ZvTLC9W7Wv933ELQ
[kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.producer.internals.TransactionManager - [Producer clientId=producer-1] ProducerId set to 12044 with epoch 0
2/3/26, 1:41:17 PM =============================================================

-- Timers ----------------------------------------------------------------------
messageMetrics
             count = 44
         mean rate = 8.67 calls/second
     1-minute rate = 8.60 calls/second
     5-minute rate = 8.60 calls/second
    15-minute rate = 8.60 calls/second
               min = 0.41 milliseconds
               max = 360.84 milliseconds
              mean = 8.59 milliseconds
            stddev = 52.69 milliseconds
            median = 0.72 milliseconds
              75% <= 0.84 milliseconds
              95% <= 0.96 milliseconds
              98% <= 360.84 milliseconds
              99% <= 360.84 milliseconds
            99.9% <= 360.84 milliseconds


2/3/26, 1:41:27 PM =============================================================

-- Timers ----------------------------------------------------------------------
messageMetrics
             count = 143
         mean rate = 9.48 calls/second
     1-minute rate = 8.80 calls/second
     5-minute rate = 8.64 calls/second
    15-minute rate = 8.61 calls/second
               min = 0.29 milliseconds
               max = 360.84 milliseconds
              mean = 2.85 milliseconds
            stddev = 28.37 milliseconds
            median = 0.58 milliseconds
              75% <= 0.69 milliseconds
              95% <= 0.88 milliseconds
              98% <= 1.15 milliseconds
              99% <= 1.19 milliseconds
            99.9% <= 360.84 milliseconds


^C[Thread-0] INFO org.apache.kafka.clients.producer.KafkaProducer - [Producer clientId=producer-1] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
[Thread-0] INFO org.apache.kafka.common.metrics.Metrics - Metrics scheduler closed
[Thread-0] INFO org.apache.kafka.common.metrics.Metrics - Closing reporter org.apache.kafka.common.metrics.JmxReporter
[Thread-0] INFO org.apache.kafka.common.metrics.Metrics - Closing reporter org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter
[Thread-0] INFO org.apache.kafka.common.metrics.Metrics - Metrics reporters closed
[Thread-0] INFO org.apache.kafka.common.utils.AppInfoParser - App info kafka.producer for producer-1 unregistered
```
