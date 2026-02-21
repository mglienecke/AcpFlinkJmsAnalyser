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
        
        // Configure environment
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(1000);
    }

    public void start() throws Exception {
        logger.info("Starting Flink Analyzer Job");

        // Create source from JMS queue
        DataStream<MessageItem> source = env
                .addSource(new JmsFlinkSource(messageQueue))
                .name("JMS Source");

        // Assign timestamps and watermarks
        DataStream<MessageItem> timestamped = source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MessageItem>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((item, timestamp) -> item.getInstant().toEpochMilli())
                )
                .name("Assign Timestamps");

        // Split stream into good and error streams
        SingleOutputStreamOperator<MessageItem> goodStream = timestamped
                .process(new StreamSplitter(ERROR_TAG))
                .name("Stream Splitter");

        DataStream<MessageItem> errorStream = goodStream.getSideOutput(ERROR_TAG);

        // Print error stream
        errorStream
                .map(item -> String.format("ERROR: %s (value=%.2f)", item.getId(), item.getValue()))
                .print("Error Stream");

        // Compute statistics on good stream with 1-minute tumbling windows
        DataStream<Statistics> statistics = goodStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(
                        new StatisticsAggregateFunction(),
                        new WindowMetadataProcessFunction()
                )
                .name("Compute Statistics");

        statistics.print("Statistics");

        // Perform clustering on good stream with 2-minute tumbling windows
        DataStream<List<ClusterResult>> clusters = goodStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(2)))
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
