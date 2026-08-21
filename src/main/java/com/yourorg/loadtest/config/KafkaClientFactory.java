package com.yourorg.loadtest.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

/**
 * Builds Kafka clients from KafkaConnectionProperties. This is the ONLY place
 * that should know how auth config maps to Kafka client Properties -- commands
 * and load generators stay auth-agnostic.
 */
@Component
public class KafkaClientFactory {

    private final KafkaConnectionProperties props;

    public KafkaClientFactory(KafkaConnectionProperties props) {
        this.props = props;
    }

    /**
     * @param groupId        consumer group id. Pass a fresh UUID-based group per load
     *                       test run if you want to always read from --from-beginning
     *                       without colliding with a previous run's committed offsets.
     * @param maxPollRecords batch size per poll() -- tune this for throughput testing.
     */
    public KafkaConsumer<byte[], byte[]> createConsumer(String groupId, int maxPollRecords) {
        Properties p = baseProperties();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null ? groupId : "loadtest-" + UUID.randomUUID());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // Client id makes individual load-test runs identifiable in broker-side
        // request logs / JMX metrics, which helps when correlating load test
        // windows with broker-side RequestQueueSize etc.
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "loadtest-consumer-" + UUID.randomUUID());
        return new KafkaConsumer<>(p);
    }

    /**
     * Plain JSON producer -- no schema registry involved. Value is sent as raw
     * UTF-8 bytes (the caller serializes the JSON payload to bytes before send);
     * key is a plain string.
     *
     * @param acks     "1" (leader ack, faster) or "all" (full ISR ack, safer)
     * @param lingerMs how long the producer batches records before sending -- higher values
     *                 trade a bit of per-record latency for much better throughput at high volume.
     */
    public KafkaProducer<String, byte[]> createJsonProducer(String acks, int lingerMs, int batchSizeBytes) {
        Properties p = baseProperties();
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        p.put(ProducerConfig.ACKS_CONFIG, acks != null ? acks : "1");
        p.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeBytes);
        // Idempotence requires acks=all (Kafka enforces this and throws ConfigException
        // otherwise) -- only turn it on when the caller actually asked for acks=all.
        // With acks=1 we skip it rather than silently overriding the caller's choice.
        boolean idempotent = "all".equalsIgnoreCase(acks);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotent));
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "loadtest-producer-" + UUID.randomUUID());
        return new KafkaProducer<>(p);
    }

    private Properties baseProperties() {
        Properties p = new Properties();
        p.put("bootstrap.servers", props.getBootstrapServers());
        p.put("security.protocol", props.getSecurityProtocol());

        boolean saslEnabled = props.getSecurityProtocol() != null
                && props.getSecurityProtocol().startsWith("SASL");

        if (saslEnabled) {
            requireNonBlank(props.getSaslJaasConfig(), "kafka.sasl.jaas-config");
            p.put("sasl.mechanism", props.getSaslMechanism());
            p.put("sasl.jaas.config", props.getSaslJaasConfig());
        }

        // Confluent Cloud (SASL_SSL) works fine with the default JVM truststore,
        // so these are only set if explicitly provided -- e.g. self-managed
        // clusters with a private CA, or mutual TLS.
        if (props.getSslTruststoreLocation() != null) {
            p.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, props.getSslTruststoreLocation());
            p.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, props.getSslTruststorePassword());
        }
        if (props.getSslKeystoreLocation() != null) {
            p.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, props.getSslKeystoreLocation());
            p.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, props.getSslKeystorePassword());
        }

        return p;
    }

    private void requireNonBlank(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Missing required property '" + propertyName + "' for SASL security protocol. "
                            + "Set it in application.yml or via env var (e.g. KAFKA_SASL_JAASCONFIG).");
        }
    }
}
