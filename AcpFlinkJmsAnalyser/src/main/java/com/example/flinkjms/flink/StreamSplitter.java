package com.example.flinkjms.flink;

import com.example.flinkjms.model.MessageItem;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process function to split stream into good and error items
 */
public class StreamSplitter extends ProcessFunction<MessageItem, MessageItem> {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamSplitter.class);
    private final OutputTag<MessageItem> errorTag;

    public StreamSplitter(OutputTag<MessageItem> errorTag) {
        this.errorTag = errorTag;
    }

    @Override
    public void processElement(MessageItem item, Context ctx, Collector<MessageItem> out) {
        logger.info("StreamSplitter processing item: id={}, value={}", item.getId(), item.getValue());

        if (item.getValue() == null) {
            logger.warn("Received item with null value: {}", item.getId());
            ctx.output(errorTag, item);
        } else if (item.isError()) {
            // value > 100, send to error stream
            logger.info("Item {} has invalid value: {} - sending to ERROR stream", item.getId(), item.getValue());
            ctx.output(errorTag, item);
        } else {
            // value <= 100, send to good stream
            logger.info("Item {} has valid value: {} - sending to GOOD stream", item.getId(), item.getValue());
            out.collect(item);
        }
    }
}
