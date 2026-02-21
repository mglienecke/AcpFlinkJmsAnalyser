package com.example.flinkjms.service;

import com.example.flinkjms.flink.FlinkAnalyzerJob;
import com.example.flinkjms.model.MessageItem;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Service to manage the Flink streaming job
 */
@Service
public class FlinkService {
    
    private static final Logger logger = LoggerFactory.getLogger(FlinkService.class);
    
    @Value("${app.flink.queue-capacity:10000}")
    private int queueCapacity;
    
    private BlockingQueue<MessageItem> messageQueue;
    private FlinkAnalyzerJob flinkJob;

    @PostConstruct
    public void initialize() {
        try {
            logger.info("Initializing Flink service with queue capacity: {}", queueCapacity);
            
            // Create blocking queue for message passing
            messageQueue = new LinkedBlockingQueue<>(queueCapacity);
            
            // Create and start Flink job
            flinkJob = new FlinkAnalyzerJob(messageQueue);
            flinkJob.start();
            
            logger.info("Flink service initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize Flink service", e);
            throw new RuntimeException("Failed to start Flink job", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Flink service");
        // Flink job will be cancelled automatically when the application stops
    }

    public BlockingQueue<MessageItem> getMessageQueue() {
        return messageQueue;
    }
}
