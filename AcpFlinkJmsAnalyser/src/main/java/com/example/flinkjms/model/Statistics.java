package com.example.flinkjms.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

public class Statistics implements Serializable {
    
    private Long windowStart;
    private Long windowEnd;
    private Long count;
    private Double average;
    private Double min;
    private Double max;
    private Double median;
    private Double standardDeviation;
    private Double sum;

    public Statistics() {}

    public Statistics(Long windowStart, Long windowEnd, Long count, Double average, Double min, Double max, Double median, Double standardDeviation, Double sum) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
        this.average = average;
        this.min = min;
        this.max = max;
        this.median = median;
        this.standardDeviation = standardDeviation;
        this.sum = sum;
    }

    public static StatisticsBuilder builder() {
        return new StatisticsBuilder();
    }

    public static class StatisticsBuilder {
        private Long windowStart;
        private Long windowEnd;
        private Long count;
        private Double average;
        private Double min;
        private Double max;
        private Double median;
        private Double standardDeviation;
        private Double sum;

        public StatisticsBuilder windowStart(Long windowStart) { this.windowStart = windowStart; return this; }
        public StatisticsBuilder windowEnd(Long windowEnd) { this.windowEnd = windowEnd; return this; }
        public StatisticsBuilder count(Long count) { this.count = count; return this; }
        public StatisticsBuilder average(Double average) { this.average = average; return this; }
        public StatisticsBuilder min(Double min) { this.min = min; return this; }
        public StatisticsBuilder max(Double max) { this.max = max; return this; }
        public StatisticsBuilder median(Double median) { this.median = median; return this; }
        public StatisticsBuilder standardDeviation(Double standardDeviation) { this.standardDeviation = standardDeviation; return this; }
        public StatisticsBuilder sum(Double sum) { this.sum = sum; return this; }

        public Statistics build() {
            return new Statistics(windowStart, windowEnd, count, average, min, max, median, standardDeviation, sum);
        }
    }

    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }
    public Long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }
    public Long getCount() { return count; }
    public void setCount(Long count) { this.count = count; }
    public Double getAverage() { return average; }
    public void setAverage(Double average) { this.average = average; }
    public Double getMin() { return min; }
    public void setMin(Double min) { this.min = min; }
    public Double getMax() { return max; }
    public void setMax(Double max) { this.max = max; }
    public Double getMedian() { return median; }
    public void setMedian(Double median) { this.median = median; }
    public Double getStandardDeviation() { return standardDeviation; }
    public void setStandardDeviation(Double standardDeviation) { this.standardDeviation = standardDeviation; }
    public Double getSum() { return sum; }
    public void setSum(Double sum) { this.sum = sum; }
    
    @Override
    public String toString() {
        return String.format(
            "Statistics[window=%s to %s, count=%d, avg=%.2f, min=%.2f, max=%.2f, median=%.2f, stdDev=%.2f]",
            Instant.ofEpochMilli(windowStart),
            Instant.ofEpochMilli(windowEnd),
            count,
            average,
            min,
            max,
            median,
            standardDeviation
        );
    }
}
