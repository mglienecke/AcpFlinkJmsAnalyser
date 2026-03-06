package com.example.flinkjms.service;

import com.example.flinkjms.model.MessageItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;

/**
 * JMS listener that receives messages and forwards them to Flink
 */
@Service
public class JmsMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(JmsMessageListener.class);

    private final FlinkService flinkService;
    private final ObjectMapper objectMapper;

    public JmsMessageListener(FlinkService flinkService, ObjectMapper objectMapper) {
        this.flinkService = flinkService;
        this.objectMapper = objectMapper;
    }

    @JmsListener(destination = "${app.jms.input-queue:input.queue}", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessage(String message) {
        try {
            logger.debug("Received JMS message: {}", message);

            // Get the queue (it should be initialized by now via @PostConstruct)
            BlockingQueue<MessageItem> messageQueue = flinkService.getMessageQueue();
            if (messageQueue == null) {
                logger.error("Message queue is not initialized yet, cannot process message");
                return;
            }

            // Parse JSON message
            MessageItem item = objectMapper.readValue(message, MessageItem.class);

            // Set timestamp if not present
            if (item.getTimestamp() == null) {
                item.setTimestamp(System.currentTimeMillis());
            }

            // Forward to Flink via queue
            boolean added = messageQueue.offer(item);
            if (!added) {
                logger.warn("Failed to add message to queue (queue full): {}", item.getId());
            } else {
                logger.info("Message forwarded to Flink: id={}, value={}", item.getId(), item.getValue());
            }

        } catch (Exception e) {
            logger.error("Error processing JMS message: {}", message, e);
        }
    }
}
