package com.example.flinkjms.flink;

import com.example.flinkjms.model.ClusterResult;
import com.example.flinkjms.model.MessageItem;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple K-means clustering aggregation function
 */
public class ClusteringAggregateFunction 
        implements AggregateFunction<MessageItem, ClusteringAggregateFunction.ClusterAccumulator, List<ClusterResult>> {

    private static final int K = 3; // Number of clusters
    private static final int MAX_ITERATIONS = 10;

    public static class ClusterAccumulator {
        List<MessageItem> items = new ArrayList<>();
    }

    @Override
    public ClusterAccumulator createAccumulator() {
        return new ClusterAccumulator();
    }

    @Override
    public ClusterAccumulator add(MessageItem item, ClusterAccumulator accumulator) {
        accumulator.items.add(item);
        return accumulator;
    }

    @Override
    public List<ClusterResult> getResult(ClusterAccumulator accumulator) {
        if (accumulator.items.size() < K) {
            return Collections.emptyList();
        }

        // Extract values
        List<Double> values = accumulator.items.stream()
                .map(MessageItem::getValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (values.isEmpty()) {
            return Collections.emptyList();
        }

        // Initialize centroids using k-means++
        List<Double> centroids = initializeCentroids(values, K);

        // Perform k-means iterations
        Map<Integer, List<MessageItem>> clusters = null;
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            // Assign items to nearest centroid
            clusters = assignToClusters(accumulator.items, centroids);

            // Update centroids
            List<Double> newCentroids = updateCentroids(clusters);
            
            // Check convergence
            if (centroidsConverged(centroids, newCentroids)) {
                break;
            }
            
            centroids = newCentroids;
        }

        // Build results
        return buildClusterResults(clusters, centroids);
    }

    @Override
    public ClusterAccumulator merge(ClusterAccumulator a, ClusterAccumulator b) {
        a.items.addAll(b.items);
        return a;
    }

    private List<Double> initializeCentroids(List<Double> values, int k) {
        List<Double> centroids = new ArrayList<>();
        Random random = new Random();
        
        // First centroid is random
        centroids.add(values.get(random.nextInt(values.size())));
        
        // Select remaining centroids using k-means++
        for (int i = 1; i < k; i++) {
            double maxDist = Double.MIN_VALUE;
            double bestCentroid = centroids.get(0);
            
            for (double value : values) {
                double minDistToCentroid = centroids.stream()
                        .mapToDouble(c -> Math.abs(value - c))
                        .min()
                        .orElse(Double.MAX_VALUE);
                
                if (minDistToCentroid > maxDist) {
                    maxDist = minDistToCentroid;
                    bestCentroid = value;
                }
            }
            
            centroids.add(bestCentroid);
        }
        
        return centroids;
    }

    private Map<Integer, List<MessageItem>> assignToClusters(List<MessageItem> items, List<Double> centroids) {
        Map<Integer, List<MessageItem>> clusters = new HashMap<>();
        for (int i = 0; i < centroids.size(); i++) {
            clusters.put(i, new ArrayList<>());
        }

        for (MessageItem item : items) {
            if (item.getValue() == null) continue;
            
            int nearestCluster = 0;
            double minDistance = Double.MAX_VALUE;
            
            for (int i = 0; i < centroids.size(); i++) {
                double distance = Math.abs(item.getValue() - centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCluster = i;
                }
            }
            
            clusters.get(nearestCluster).add(item);
        }

        return clusters;
    }

    private List<Double> updateCentroids(Map<Integer, List<MessageItem>> clusters) {
        List<Double> newCentroids = new ArrayList<>();
        
        for (int i = 0; i < clusters.size(); i++) {
            List<MessageItem> clusterItems = clusters.get(i);
            if (clusterItems.isEmpty()) {
                newCentroids.add(0.0);
            } else {
                double avg = clusterItems.stream()
                        .mapToDouble(MessageItem::getValue)
                        .average()
                        .orElse(0.0);
                newCentroids.add(avg);
            }
        }
        
        return newCentroids;
    }

    private boolean centroidsConverged(List<Double> old, List<Double> newCentroids) {
        double threshold = 0.01;
        for (int i = 0; i < old.size(); i++) {
            if (Math.abs(old.get(i) - newCentroids.get(i)) > threshold) {
                return false;
            }
        }
        return true;
    }

    private List<ClusterResult> buildClusterResults(Map<Integer, List<MessageItem>> clusters, 
                                                    List<Double> centroids) {
        List<ClusterResult> results = new ArrayList<>();
        
        for (int i = 0; i < clusters.size(); i++) {
            List<MessageItem> items = clusters.get(i);
            if (items.isEmpty()) continue;
            
            List<String> sampleIds = items.stream()
                    .limit(5)
                    .map(MessageItem::getId)
                    .collect(Collectors.toList());
            
            results.add(ClusterResult.builder()
                    .clusterId(i)
                    .centroidValue(centroids.get(i))
                    .itemCount((long) items.size())
                    .sampleIds(sampleIds)
                    .build());
        }
        
        return results;
    }
}
