# Flink JMS Analyzer

A Spring Boot application that integrates JMS (Java Message Service) with Apache Flink for real-time stream processing and analytics.

## Features

### Stream Processing
- **JMS Integration**: Receives JSON messages from JMS queues (ActiveMQ)
- **Stream Splitting**: Automatically separates messages into:
  - **Good Stream**: Values >= 0 and <= 100
  - **Error Stream**: Values > 100

### Real-time Analytics

#### Statistical Analysis
Computed over 1-minute tumbling windows:
- **Average** (mean)
- **Minimum** value
- **Maximum** value
- **Median** (50th percentile)
- **Standard Deviation**
- **Count** of items
- **Sum** of all values

#### Clustering Analysis
K-means clustering over 2-minute tumbling windows:
- Groups similar values into 3 clusters
- Identifies cluster centroids
- Tracks cluster membership
- Uses k-means++ initialization for better convergence

## Architecture

```
JMS Queue (ActiveMQ)
    ↓
JmsMessageListener (Spring)
    ↓
BlockingQueue (Bridge)
    ↓
Flink Streaming Job
    ↓
Stream Splitter
    ├─→ Good Stream (0 <= value <= 100)
    │       ├─→ Statistics Aggregation
    │       └─→ Clustering Analysis
    └─→ Error Stream (value > 100)
```

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- Apache ActiveMQ (or any JMS broker)

## Setup

### 1. Install and Start ActiveMQ

Download from: https://activemq.apache.org/components/classic/download/

```bash
cd apache-activemq-<version>
bin/activemq start
```

ActiveMQ Web Console: http://localhost:8161/admin (admin/admin)

### 2. Build the Project

```bash
mvn clean install
```

### 3. Run the Application

```bash
mvn spring-boot:run
```

### 4. Send Test Messages

In a separate terminal, run the test producer:

```bash
mvn test-compile exec:java -Dexec.mainClass="com.example.flinkjms.TestMessageProducer"
```

## Configuration

Edit `src/main/resources/application.yml`:

```yaml
spring:
  activemq:
    broker-url: tcp://localhost:61616
    user: admin
    password: admin

app:
  jms:
    input-queue: input.queue
  flink:
    queue-capacity: 10000
```

## Message Format

Messages should be JSON with the following structure:

```json
{
  "id": "unique-id",
  "value": 75.5,
  "timestamp": 1234567890000,
  "metadata": "optional-metadata"
}
```

### Fields
- `id` (String): Unique identifier for the message
- `value` (Double): The numeric value to analyze
  - **Valid**: 0 <= value <= 100 → Good stream
  - **Invalid**: value > 100 or value < 0 → Error stream
- `timestamp` (Long): Unix timestamp in milliseconds (auto-generated if missing)
- `metadata` (String): Optional metadata

## Output

### Console Output

**Error Stream:**
```
Error Stream> ERROR: abc123 (value=150.00)
Error Stream> ERROR: def456 (value=-25.00)
```

**Statistics (every 1 minute):**
```
Statistics> Statistics[window=2024-01-15T10:00:00Z to 2024-01-15T10:01:00Z, 
            count=120, avg=50.25, min=0.50, max=99.80, median=48.90, stdDev=28.75]
```

**Clusters (every 2 minutes):**
```
Clusters> Cluster[id=0, centroid=25.50, items=40, samples=[abc, def, ghi]]
Clusters> Cluster[id=1, centroid=50.25, items=45, samples=[jkl, mno, pqr]]
Clusters> Cluster[id=2, centroid=75.80, items=35, samples=[stu, vwx, yza]]
```

## Project Structure

```
src/main/java/com/example/flinkjms/
├── FlinkJmsAnalyzerApplication.java    # Main Spring Boot application
├── config/
│   ├── ApplicationConfig.java          # General app configuration
│   └── JmsConfig.java                  # JMS configuration
├── model/
│   ├── MessageItem.java                # Message data model
│   ├── Statistics.java                 # Statistics result model
│   └── ClusterResult.java              # Clustering result model
├── service/
│   ├── JmsMessageListener.java         # JMS listener
│   └── FlinkService.java               # Flink job manager
└── flink/
    ├── FlinkAnalyzerJob.java           # Main Flink job
    ├── JmsFlinkSource.java             # Flink source for JMS
    ├── StreamSplitter.java             # Good/Error stream splitter
    ├── StatisticsAggregateFunction.java # Statistics computation
    ├── ClusteringAggregateFunction.java # K-means clustering
    └── WindowMetadataProcessFunction.java # Window metadata
```

## Key Components

### Stream Processing Pipeline

1. **JmsMessageListener**: Receives messages from JMS queue
2. **BlockingQueue**: Thread-safe bridge between Spring and Flink
3. **JmsFlinkSource**: Flink source that reads from the queue
4. **StreamSplitter**: Routes messages to good/error streams based on value
5. **Aggregation Functions**: Compute statistics and clusters over time windows

### Window Configuration

- **Statistics Window**: 1-minute tumbling windows
- **Clustering Window**: 2-minute tumbling windows
- **Watermark Strategy**: 5-second bounded out-of-orderness

### Clustering Algorithm

- **Algorithm**: K-means with k-means++ initialization
- **Number of Clusters**: 3 (configurable in code)
- **Max Iterations**: 10
- **Convergence Threshold**: 0.01

## Extending the Application

### Add More Analytics

Create a new aggregate function implementing `AggregateFunction`:

```java
public class MyAnalytics implements AggregateFunction<MessageItem, MyAccumulator, MyResult> {
    // Implementation
}
```

Then add to the Flink job:

```java
DataStream<MyResult> myAnalytics = goodStream
    .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new MyAnalytics(), new WindowMetadataProcessFunction<>());
```

### Custom Validation Rules

Modify `StreamSplitter.java` to change validation logic:

```java
if (item.getValue() > CUSTOM_THRESHOLD) {
    ctx.output(errorTag, item);
} else {
    out.collect(item);
}
```

### Persist Results

Add a sink to store results in a database:

```java
statistics.addSink(new JdbcSink<>(/* configuration */));
```

## Testing

### Unit Tests

```bash
mvn test
```

### Integration Testing

1. Start ActiveMQ
2. Run the main application
3. Run the test producer
4. Observe console output for statistics and clusters

### Manual Message Sending

Use ActiveMQ Web Console or send via code:

```java
jmsTemplate.convertAndSend("input.queue", 
    "{\"id\":\"test1\",\"value\":50.0,\"timestamp\":1234567890000}");
```

## Performance Tuning

### Flink Configuration

In `FlinkAnalyzerJob.java`:

```java
env.setParallelism(8);  // Increase parallelism
env.getConfig().setAutoWatermarkInterval(500);  // Faster watermarks
```

### JMS Configuration

In `JmsConfig.java`:

```java
factory.setConcurrency("5-20");  // More concurrent consumers
```

### Queue Capacity

In `application.yml`:

```yaml
app:
  flink:
    queue-capacity: 50000  # Larger buffer
```

## Troubleshooting

### Messages not being processed
- Check ActiveMQ is running: `http://localhost:8161/admin`
- Verify queue name matches configuration
- Check application logs for errors

### Out of Memory
- Increase JVM heap: `mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx2g"`
- Reduce window sizes
- Decrease queue capacity

### Slow Processing
- Increase Flink parallelism
- Add more JMS concurrent consumers
- Optimize aggregation functions

## Dependencies

- Spring Boot 3.2.1
- Apache Flink 1.18.0
- Apache ActiveMQ
- Apache Commons Math 3.6.1
- Jackson 2.15.3
- Lombok

## License

MIT License

## Contributing

Contributions welcome! Please submit pull requests or open issues for bugs and feature requests.
