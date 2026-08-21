package com.yourorg.loadtest.command;

import com.yourorg.loadtest.config.KafkaClientFactory;
import com.yourorg.loadtest.engine.ConsumerLoadGenerator;
import com.yourorg.loadtest.metrics.ConsumerLoadTestReport;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.time.Duration;

@ShellComponent
public class ConsumerLoadCommand {

    private final KafkaClientFactory clientFactory;

    public ConsumerLoadCommand(KafkaClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @ShellMethod(key = "consumer-test", value = "Run a consumer load test against a topic on the configured broker")
    public String consumerTest(
            @ShellOption(value = "--topic", help = "Topic to consume from") String topic,
            @ShellOption(value = "--duration-seconds", defaultValue = "60", help = "How long to run the test") int durationSeconds,
            @ShellOption(value = "--warmup-seconds", defaultValue = "5", help = "Warmup window excluded from metrics") int warmupSeconds,
            @ShellOption(value = "--max-poll-records", defaultValue = "500", help = "max.poll.records for the consumer") int maxPollRecords,
            @ShellOption(value = "--poll-timeout-ms", defaultValue = "100", help = "poll() timeout in ms") long pollTimeoutMs,
            @ShellOption(value = "--group-id", defaultValue = ShellOption.NULL, help = "Consumer group id; random UUID-based id if omitted") String groupId
    ) {
        if (durationSeconds <= warmupSeconds) {
            return "Error: --duration-seconds must be greater than --warmup-seconds";
        }

        KafkaConsumer<byte[], byte[]> consumer = clientFactory.createConsumer(groupId, maxPollRecords);
        ConsumerLoadGenerator generator = new ConsumerLoadGenerator(consumer);

        // Ctrl+C during an interactive shell run stops the test cleanly and still
        // prints whatever metrics were collected so far, rather than losing them.
        Runtime.getRuntime().addShutdownHook(new Thread(generator::stop));

        ConsumerLoadTestReport report = generator.run(
                topic,
                Duration.ofSeconds(durationSeconds),
                Duration.ofMillis(pollTimeoutMs),
                warmupSeconds
        );

        return report.toSummaryString();
    }
}
