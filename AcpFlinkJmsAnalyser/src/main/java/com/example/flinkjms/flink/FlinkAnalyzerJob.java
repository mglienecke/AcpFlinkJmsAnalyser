package com.example.flinkjms.flink;

import com.example.flinkjms.model.ClusterResult;
import com.example.flinkjms.model.MessageItem;
import com.example.flinkjms.model.Statistics;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Main Flink job for processing JMS messages
 */
public class FlinkAnalyzerJob {
    
    private static final Logger logger = LoggerFactory.getLogger(FlinkAnalyzerJob.class);
    
    // Output tags for side outputs
    private static final OutputTag<MessageItem> ERROR_TAG = new OutputTag<MessageItem>("error-stream"){};
    
    private final StreamExecutionEnvironment env;
    private final BlockingQueue<MessageItem> messageQueue;

    public FlinkAnalyzerJob(BlockingQueue<MessageItem> messageQueue) {
        this.messageQueue = messageQueue;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure environment - use parallelism 1 for simpler debugging
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        logger.info("Flink environment configured: parallelism=1, watermark interval=1000ms");
    }

    public void start() throws Exception {
        logger.info("Starting Flink Analyzer Job");

        // Create source from JMS queue
        DataStream<MessageItem> source = env
                .addSource(new JmsFlinkSource(messageQueue))
                .name("JMS Source")
                .map(item -> {
                    logger.info("Source emitted: {}", item);
                    return item;
                });

        // Assign timestamps and watermarks with idle timeout to allow watermarks to advance
        DataStream<MessageItem> timestamped = source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MessageItem>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((item, timestamp) -> {
                                    long ts = item.getInstant().toEpochMilli();
                                    logger.info("Assigning timestamp {} to item {}", ts, item.getId());
                                    return ts;
                                })
                                .withIdleness(Duration.ofSeconds(5))
                )
                .name("Assign Timestamps")
                .map(item -> {
                    logger.info("After timestamp assignment: {}", item);
                    return item;
                });

        // Split stream into good and error streams
        SingleOutputStreamOperator<MessageItem> goodStream = timestamped
                .process(new StreamSplitter(ERROR_TAG))
                .name("Stream Splitter")
                .map(item -> {
                    logger.info("Good stream item: {}", item);
                    return item;
                });

        DataStream<MessageItem> errorStream = goodStream.getSideOutput(ERROR_TAG);

        // Print error stream IMMEDIATELY (no windowing)
        errorStream
                .map(item -> String.format("ERROR: id=%s, value=%.2f, timestamp=%d",
                        item.getId(), item.getValue(), item.getTimestamp()))
                .print("Error Stream");

        // Also print good stream immediately for debugging
        goodStream
                .map(item -> String.format("VALID: id=%s, value=%.2f, timestamp=%d",
                        item.getId(), item.getValue(), item.getTimestamp()))
                .print("Valid Stream");

        // Compute statistics on good stream with 10-second tumbling windows (for faster results)
        DataStream<Statistics> statistics = goodStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new StatisticsAggregateFunction(),
                        new WindowMetadataProcessFunction()
                )
                .name("Compute Statistics");

        statistics.print("Statistics");

        // Perform clustering on good stream with 20-second tumbling windows (for faster results)
        DataStream<List<ClusterResult>> clusters = goodStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .aggregate(
                        new ClusteringAggregateFunction(),
                        new WindowMetadataProcessFunction<>()
                )
                .name("Clustering");

        clusters
                .flatMap((List<ClusterResult> results, org.apache.flink.util.Collector<String> out) -> {
                    for (ClusterResult result : results) {
                        out.collect(result.toString());
                    }
                })
                .returns(String.class)
                .print("Clusters");

        // Execute the job asynchronously
        env.executeAsync("Flink JMS Analyzer");
        logger.info("Flink job started successfully");
    }
}
