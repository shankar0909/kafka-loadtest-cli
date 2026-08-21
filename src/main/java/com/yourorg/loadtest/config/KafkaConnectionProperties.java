package com.yourorg.loadtest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Broker connection settings, bound from application.yml under the "kafka" prefix.
 *
 * Every field is overridable via env var using Spring's relaxed binding, e.g.:
 *   KAFKA_BOOTSTRAPSERVERS=pkc-xxxxx.confluent.cloud:9092
 *   KAFKA_SASL_JAASCONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="KEY" password="SECRET";'
 *
 * This mirrors the property-override / env-var-injection pattern used in the
 * core toolkit library, so CI can inject credentials without touching yml files.
 */
@ConfigurationProperties(prefix = "kafka")
public class KafkaConnectionProperties {

    /** e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 */
    private String bootstrapServers = "localhost:9092";

    /** PLAINTEXT | SSL | SASL_SSL | SASL_PLAINTEXT. Confluent Cloud uses SASL_SSL. */
    private String securityProtocol = "SASL_SSL";

    /** PLAIN for Confluent Cloud API keys; SCRAM-SHA-256/512 for self-managed SASL/SCRAM. */
    private String saslMechanism = "PLAIN";

    /** Full JAAS config string, e.g. org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";
     *  Kept as one string (rather than separate key/secret fields) because that's the literal
     *  property the Kafka client expects -- no translation layer to get wrong. */
    private String saslJaasConfig;

    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;

    /** Optional: Confluent Cloud generally just needs SASL_SSL with the default JVM
     *  truststore, so these SSL fields are usually left null for Confluent Cloud and
     *  only populated for self-managed clusters with custom/private CAs or mTLS. */

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }
}
