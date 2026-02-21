package com.example.flinkjms.flink;

import com.example.flinkjms.model.ClusterResult;
import com.example.flinkjms.model.Statistics;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Process window function to add window metadata to aggregated results
 */
public class WindowMetadataProcessFunction<T> extends ProcessAllWindowFunction<T, T, TimeWindow> {

    @Override
    public void process(Context context, Iterable<T> elements, Collector<T> out) {
        TimeWindow window = context.window();
        
        for (T element : elements) {
            // Add window metadata
            if (element instanceof Statistics) {
                Statistics stats = (Statistics) element;
                stats.setWindowStart(window.getStart());
                stats.setWindowEnd(window.getEnd());
            } else if (element instanceof List) {
                // For cluster results
                @SuppressWarnings("unchecked")
                List<ClusterResult> clusters = (List<ClusterResult>) element;
                for (ClusterResult cluster : clusters) {
                    cluster.setWindowStart(window.getStart());
                    cluster.setWindowEnd(window.getEnd());
                }
            }
            
            out.collect(element);
        }
    }
}
