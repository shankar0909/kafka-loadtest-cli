package com.yourorg.loadtest;

import com.yourorg.loadtest.config.KafkaConnectionProperties;
import com.yourorg.loadtest.config.LoadTestProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({KafkaConnectionProperties.class, LoadTestProperties.class})
public class LoadTestCliApplication {

    public static void main(String[] args) {
        // Non-interactive mode: if args are passed (e.g. "consumer-test --topic foo"),
        // Spring Shell runs that single command and exits -- ideal for CI.
        // No args -> drops into the interactive shell.
        SpringApplication.run(LoadTestCliApplication.class, args);
    }
}
