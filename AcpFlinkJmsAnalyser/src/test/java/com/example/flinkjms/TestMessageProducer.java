package com.example.flinkjms;

import com.example.flinkjms.model.MessageItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.core.JmsTemplate;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test producer to send sample messages to JMS queue
 * Run this separately to generate test data
 */
@SpringBootApplication
public class TestMessageProducer {

    public static void main(String[] args) {
        SpringApplication.run(TestMessageProducer.class, args);
    }

    @Bean
    public CommandLineRunner sendTestMessages(JmsTemplate jmsTemplate, ObjectMapper objectMapper) {
        return args -> {
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            Random random = new Random();

            executor.scheduleAtFixedRate(() -> {
                try {
                    // Generate random message
                    MessageItem item = new MessageItem();
                    item.setId(UUID.randomUUID().toString().substring(0, 8));
                    item.setTimestamp(System.currentTimeMillis());
                    item.setMetadata("test-metadata");

                    // 70% valid (0-100), 20% invalid (>100), 10% negative
                    double rand = random.nextDouble();
                    if (rand < 0.7) {
                        // Valid values: 0-100
                        item.setValue(random.nextDouble() * 100);
                    } else if (rand < 0.9) {
                        // Invalid values: 101-200
                        item.setValue(101 + random.nextDouble() * 99);
                    } else {
                        // Negative values
                        item.setValue(-random.nextDouble() * 50);
                    }

                    // Send to JMS
                    String json = objectMapper.writeValueAsString(item);
                    jmsTemplate.convertAndSend("input.queue", json);
                    
                    System.out.printf("Sent: id=%s, value=%.2f%n", item.getId(), item.getValue());

                } catch (Exception e) {
                    System.err.println("Error sending message: " + e.getMessage());
                }
            }, 0, 500, TimeUnit.MILLISECONDS); // Send message every 500ms

            System.out.println("Test message producer started. Sending messages every 500ms...");
            System.out.println("Press Ctrl+C to stop.");
        };
    }
}
