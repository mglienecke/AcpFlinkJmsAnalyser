package com.example.flinkjms.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

public class ClusterResult implements Serializable {
    
    private Long windowStart;
    private Long windowEnd;
    private Integer clusterId;
    private Double centroidValue;
    private Long itemCount;
    private List<String> sampleIds;

    public ClusterResult() {}

    public ClusterResult(Long windowStart, Long windowEnd, Integer clusterId, Double centroidValue, Long itemCount, List<String> sampleIds) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.clusterId = clusterId;
        this.centroidValue = centroidValue;
        this.itemCount = itemCount;
        this.sampleIds = sampleIds;
    }

    public static ClusterResultBuilder builder() {
        return new ClusterResultBuilder();
    }

    public static class ClusterResultBuilder {
        private Long windowStart;
        private Long windowEnd;
        private Integer clusterId;
        private Double centroidValue;
        private Long itemCount;
        private List<String> sampleIds;

        public ClusterResultBuilder windowStart(Long windowStart) { this.windowStart = windowStart; return this; }
        public ClusterResultBuilder windowEnd(Long windowEnd) { this.windowEnd = windowEnd; return this; }
        public ClusterResultBuilder clusterId(Integer clusterId) { this.clusterId = clusterId; return this; }
        public ClusterResultBuilder centroidValue(Double centroidValue) { this.centroidValue = centroidValue; return this; }
        public ClusterResultBuilder itemCount(Long itemCount) { this.itemCount = itemCount; return this; }
        public ClusterResultBuilder sampleIds(List<String> sampleIds) { this.sampleIds = sampleIds; return this; }

        public ClusterResult build() {
            return new ClusterResult(windowStart, windowEnd, clusterId, centroidValue, itemCount, sampleIds);
        }
    }

    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }
    public Long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }
    public Integer getClusterId() { return clusterId; }
    public void setClusterId(Integer clusterId) { this.clusterId = clusterId; }
    public Double getCentroidValue() { return centroidValue; }
    public void setCentroidValue(Double centroidValue) { this.centroidValue = centroidValue; }
    public Long getItemCount() { return itemCount; }
    public void setItemCount(Long itemCount) { this.itemCount = itemCount; }
    public List<String> getSampleIds() { return sampleIds; }
    public void setSampleIds(List<String> sampleIds) { this.sampleIds = sampleIds; }
    
    @Override
    public String toString() {
        return String.format(
            "Cluster[id=%d, centroid=%.2f, items=%d, samples=%s]",
            clusterId,
            centroidValue,
            itemCount,
            sampleIds != null ? sampleIds.subList(0, Math.min(3, sampleIds.size())) : "[]"
        );
    }
}
