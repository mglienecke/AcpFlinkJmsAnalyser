```mermaid
graph TB
    subgraph "External Systems"
        JMS[JMS Queue - ActiveMQ<br/>Topic: input.queue]
    end

    subgraph "Spring Boot Application"
        Listener[JmsMessageListener<br/>@JmsListener]
        Queue[BlockingQueue<br/>Thread-Safe Bridge]
        FlinkSvc[FlinkService<br/>Lifecycle Manager]
    end

    subgraph "Apache Flink Streaming"
        Source[JmsFlinkSource<br/>Custom Source Function]
        Timestamp[Timestamp Assignment<br/>Watermark Strategy]
        Splitter[StreamSplitter<br/>Process Function]
        
        subgraph "Good Stream - 0 ≤ value ≤ 100"
            Stats[Statistics Window<br/>1-minute tumbling]
            StatsAgg[StatisticsAggregateFunction<br/>avg, min, max, median, stdDev]
            
            Cluster[Clustering Window<br/>2-minute tumbling]
            ClusterAgg[ClusteringAggregateFunction<br/>K-means k=3]
        end
        
        subgraph "Error Stream - value > 100"
            Error[Error Handler<br/>Log & Alert]
        end
    end

    subgraph "Output"
        Console[Console Output]
        StatsOut[Statistics Results]
        ClusterOut[Cluster Results]
        ErrorOut[Error Notifications]
    end

    JMS -->|JSON Messages| Listener
    Listener -->|Parse & Validate| Queue
    Queue -->|Feed Messages| FlinkSvc
    FlinkSvc -->|Initialize| Source
    Source -->|DataStream| Timestamp
    Timestamp -->|With Watermarks| Splitter
    
    Splitter -->|Valid: 0-100| Stats
    Splitter -->|Valid: 0-100| Cluster
    Splitter -->|Invalid: >100| Error
    
    Stats -->|Aggregate| StatsAgg
    Cluster -->|Aggregate| ClusterAgg
    
    StatsAgg -->|Every 1 min| StatsOut
    ClusterAgg -->|Every 2 min| ClusterOut
    Error -->|Immediate| ErrorOut
    
    StatsOut --> Console
    ClusterOut --> Console
    ErrorOut --> Console

    style JMS fill:#e1f5ff
    style Listener fill:#ffe1e1
    style Queue fill:#fff4e1
    style Source fill:#e1ffe1
    style Splitter fill:#ffe1f5
    style Stats fill:#f0e1ff
    style Cluster fill:#f0e1ff
    style Error fill:#ffe1e1
```

## Data Flow Example

### Sample Input Message
```json
{
  "id": "msg-001",
  "value": 75.5,
  "timestamp": 1705320000000,
  "metadata": "sensor-reading"
}
```

### Processing Path
1. **JMS → Spring**: Message arrives at ActiveMQ queue
2. **Spring → Flink**: JmsMessageListener parses JSON and puts in BlockingQueue
3. **Flink Source**: Reads from queue and emits to stream
4. **Timestamp Assignment**: Extracts timestamp (1705320000000) and generates watermarks
5. **Stream Splitter**: 
   - `value = 75.5` → Valid (0-100) → Good Stream
   - `value = 150` → Invalid (>100) → Error Stream
6. **Window Processing**:
   - Statistics: Aggregates values every 1 minute
   - Clustering: Groups similar values every 2 minutes

### Sample Outputs

**Statistics Output** (every 1 minute):
```
Statistics[
  window=2024-01-15T10:00:00Z to 2024-01-15T10:01:00Z,
  count=120,
  avg=50.25,
  min=0.50,
  max=99.80,
  median=48.90,
  stdDev=28.75
]
```

**Clustering Output** (every 2 minutes):
```
Cluster[id=0, centroid=25.50, items=40, samples=[abc, def, ghi]]
Cluster[id=1, centroid=50.25, items=45, samples=[jkl, mno, pqr]]
Cluster[id=2, centroid=75.80, items=35, samples=[stu, vwx, yza]]
```

**Error Output** (immediate):
```
ERROR: msg-003 (value=150.00)
```
