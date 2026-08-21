package com.yourorg.loadtest.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourorg.loadtest.config.KafkaClientFactory;
import com.yourorg.loadtest.config.LoadTestProperties;
import com.yourorg.loadtest.config.LoadTestProperties.EventTypeSettings;
import com.yourorg.loadtest.engine.JsonEventFactory;
import com.yourorg.loadtest.engine.JsonTemplateLoader;
import com.yourorg.loadtest.engine.ProducerLoadGenerator;
import com.yourorg.loadtest.metrics.ProducerLoadTestReport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.time.Duration;

/**
 * producer-test is driven entirely by application.yml -- "loadtest.events.<event-type>".
 * The event-type name doubles as the JSON template's short name (events/<event-type>.json)
 * and the yaml config key, so adding a new event type is: drop the .json file,
 * add one yaml block, done -- no new command, no code changes.
 *
 * All the CLI flags below are OPTIONAL overrides for one-off tweaks; if omitted,
 * everything comes from the matching yaml block.
 */
@ShellComponent
public class ProducerLoadCommand {

    private final KafkaClientFactory clientFactory;
    private final LoadTestProperties loadTestProperties;
    private final JsonTemplateLoader templateLoader;
    private final ObjectMapper objectMapper;

    public ProducerLoadCommand(KafkaClientFactory clientFactory, LoadTestProperties loadTestProperties, ObjectMapper objectMapper) {
        this.clientFactory = clientFactory;
        this.loadTestProperties = loadTestProperties;
        this.objectMapper = objectMapper;
        this.templateLoader = new JsonTemplateLoader(objectMapper);
    }

    @ShellMethod(key = "producer-test", value = "Run a producer load test for an event type, configured via loadtest.events.<event-type> in application.yml")
    public String producerTest(
            @ShellOption(value = "--event-type",
                    help = "Event type to test, e.g. \"user-created\" or \"order-placed\". "
                            + "Must match both an events/<event-type>.json template and a loadtest.events.<event-type> yaml block") String eventType,

            // Everything below is an optional override of the matching yaml block --
            // leave unset to use whatever's configured in application.yml.
            @ShellOption(value = "--topic", defaultValue = ShellOption.NULL) String topicOverride,
            @ShellOption(value = "--num-records", defaultValue = ShellOption.NULL) Long numRecordsOverride,
            @ShellOption(value = "--duration-seconds", defaultValue = ShellOption.NULL) Integer durationSecondsOverride,
            @ShellOption(value = "--rate-per-second", defaultValue = ShellOption.NULL) Double ratePerSecondOverride,
            @ShellOption(value = "--acks", defaultValue = ShellOption.NULL) String acksOverride,
            @ShellOption(value = "--linger-ms", defaultValue = ShellOption.NULL) Integer lingerMsOverride,
            @ShellOption(value = "--batch-size-bytes", defaultValue = ShellOption.NULL) Integer batchSizeBytesOverride,
            @ShellOption(value = "--key-field", defaultValue = ShellOption.NULL) String keyFieldOverride
    ) {
        EventTypeSettings settings = loadTestProperties.getEvents().get(eventType);
        if (settings == null) {
            String configured = String.join(", ", loadTestProperties.getEvents().keySet());
            return "Error: no loadtest.events." + eventType + " block found in application.yml. "
                    + "Configured event types: [" + configured + "]. "
                    + "Add a block for '" + eventType + "' (see README) or check for a typo.";
        }

        String topic = topicOverride != null ? topicOverride : settings.getTopic();
        long numRecords = numRecordsOverride != null ? numRecordsOverride : settings.getNumRecords();
        int durationSeconds = durationSecondsOverride != null ? durationSecondsOverride : settings.getDurationSeconds();
        double ratePerSecond = ratePerSecondOverride != null ? ratePerSecondOverride : settings.getRatePerSecond();
        String acks = acksOverride != null ? acksOverride : settings.getAcks();
        int lingerMs = lingerMsOverride != null ? lingerMsOverride : settings.getLingerMs();
        int batchSizeBytes = batchSizeBytesOverride != null ? batchSizeBytesOverride : settings.getBatchSizeBytes();
        String keyField = keyFieldOverride != null ? keyFieldOverride : settings.getKeyField();

        if (topic == null || topic.isBlank()) {
            return "Error: no topic configured for event type '" + eventType
                    + "'. Set loadtest.events." + eventType + ".topic in application.yml, or pass --topic.";
        }
        if (numRecords <= 0 && durationSeconds <= 0) {
            return "Error: for event type '" + eventType + "', set either num-records (> 0) "
                    + "or a positive duration-seconds in application.yml, or override with --num-records/--duration-seconds.";
        }

        JsonNode template;
        try {
            // Event type name doubles as the template's short name -- resolves to events/<eventType>.json.
            template = templateLoader.load(eventType);
        } catch (Exception e) {
            return "Error loading JSON template for event type '" + eventType + "': " + e.getMessage();
        }

        KafkaProducer<String, byte[]> producer = clientFactory.createJsonProducer(acks, lingerMs, batchSizeBytes);
        ProducerLoadGenerator generator = new ProducerLoadGenerator(producer, new JsonEventFactory(template), objectMapper);

        // Ctrl+C stops the run cleanly (flushes in-flight sends) and still
        // reports whatever was sent so far, rather than losing the numbers.
        Runtime.getRuntime().addShutdownHook(new Thread(generator::stop));

        ProducerLoadTestReport report;
        try {
            report = generator.run(topic, numRecords, Duration.ofSeconds(durationSeconds), ratePerSecond, keyField);
        } catch (IllegalArgumentException e) {
            // e.g. keyField referencing a field that doesn't exist on this event, or an unknown {{token}}
            return "Error: " + e.getMessage();
        }

        return "Event type: " + eventType + "\n" + report.toSummaryString();
    }
}
