package com.example.flinkjms.flink;

import com.example.flinkjms.model.MessageItem;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Flink source that receives messages from JMS via a blocking queue
 */
public class JmsFlinkSource implements SourceFunction<MessageItem> {
    
    private static final Logger logger = LoggerFactory.getLogger(JmsFlinkSource.class);
    private final BlockingQueue<MessageItem> queue;
    private volatile boolean running = true;

    public JmsFlinkSource(BlockingQueue<MessageItem> queue) {
        this.queue = queue;
    }

    @Override
    public void run(SourceContext<MessageItem> ctx) throws Exception {
        logger.info("JmsFlinkSource started");
        
        while (running) {
            try {
                MessageItem item = queue.take();
                ctx.collect(item);
            } catch (InterruptedException e) {
                if (running) {
                    logger.error("Interrupted while waiting for messages", e);
                }
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("JmsFlinkSource stopped");
    }

    @Override
    public void cancel() {
        logger.info("Cancelling JmsFlinkSource");
        running = false;
    }
}
