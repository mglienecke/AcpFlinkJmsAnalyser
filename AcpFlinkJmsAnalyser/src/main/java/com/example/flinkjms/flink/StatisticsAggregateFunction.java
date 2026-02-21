package com.example.flinkjms.flink;

import com.example.flinkjms.model.MessageItem;
import com.example.flinkjms.model.Statistics;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function to compute statistics over a window of MessageItems
 */
public class StatisticsAggregateFunction 
        implements AggregateFunction<MessageItem, DescriptiveStatistics, Statistics> {

    @Override
    public DescriptiveStatistics createAccumulator() {
        return new DescriptiveStatistics();
    }

    @Override
    public DescriptiveStatistics add(MessageItem item, DescriptiveStatistics accumulator) {
        if (item.getValue() != null) {
            accumulator.addValue(item.getValue());
        }
        return accumulator;
    }

    @Override
    public Statistics getResult(DescriptiveStatistics accumulator) {
        if (accumulator.getN() == 0) {
            return Statistics.builder()
                    .count(0L)
                    .build();
        }

        return Statistics.builder()
                .count(accumulator.getN())
                .average(accumulator.getMean())
                .min(accumulator.getMin())
                .max(accumulator.getMax())
                .median(accumulator.getPercentile(50))
                .standardDeviation(accumulator.getStandardDeviation())
                .sum(accumulator.getSum())
                .build();
    }

    @Override
    public DescriptiveStatistics merge(DescriptiveStatistics a, DescriptiveStatistics b) {
        // Merge two accumulators
        double[] bValues = b.getValues();
        for (double value : bValues) {
            a.addValue(value);
        }
        return a;
    }
}
