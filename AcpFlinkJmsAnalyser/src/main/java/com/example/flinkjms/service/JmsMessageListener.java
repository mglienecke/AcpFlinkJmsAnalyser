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
    
    private final BlockingQueue<MessageItem> messageQueue;
    private final ObjectMapper objectMapper;

    public JmsMessageListener(BlockingQueue<MessageItem> messageQueue, ObjectMapper objectMapper) {
        this.messageQueue = messageQueue;
        this.objectMapper = objectMapper;
    }

    @JmsListener(destination = "${app.jms.input-queue:input.queue}", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessage(String message) {
        try {
            logger.debug("Received JMS message: {}", message);
            
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
                logger.debug("Message forwarded to Flink: {}", item.getId());
            }
            
        } catch (Exception e) {
            logger.error("Error processing JMS message: {}", message, e);
        }
    }
}
