#!/bin/bash

echo "========================================="
echo "Flink JMS Analyzer - Quick Start"
echo "========================================="
echo ""

# Check if docker is available
if command -v docker &> /dev/null && command -v docker-compose &> /dev/null; then
    echo "Step 1: Starting ActiveMQ with Docker..."
    docker-compose up -d
    echo "✓ ActiveMQ started"
    echo "  Web Console: http://localhost:8161/admin (admin/admin)"
    echo ""
    echo "Waiting for ActiveMQ to be ready..."
    sleep 10
else
    echo "⚠ Docker not found. Please start ActiveMQ manually:"
    echo "  Download from: https://activemq.apache.org/components/classic/download/"
    echo "  Then run: bin/activemq start"
    echo ""
    read -p "Press Enter when ActiveMQ is running..."
fi

echo "Step 2: Building the application..."
mvn clean package -DskipTests
if [ $? -eq 0 ]; then
    echo "✓ Build successful"
else
    echo "✗ Build failed"
    exit 1
fi

echo ""
echo "Step 3: Starting the main application..."
echo "  (Check console for statistics and clustering output)"
echo ""

# Start the application in background
mvn spring-boot:run &
APP_PID=$!
echo "  Application PID: $APP_PID"

echo ""
echo "Waiting for application to start..."
sleep 15

echo ""
echo "Step 4: Starting test message producer..."
echo "  (Sending test messages every 500ms)"
echo ""

mvn test-compile exec:java -Dexec.mainClass="com.example.flinkjms.TestMessageProducer"

echo ""
echo "========================================="
echo "Quick Start Complete!"
echo "========================================="
echo ""
echo "Press Ctrl+C to stop all services"
