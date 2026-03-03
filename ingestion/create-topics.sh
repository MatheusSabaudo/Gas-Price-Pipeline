#!/bin/bash
# ingestion/scripts/create-topics.sh

echo "=============================================="
echo " Creating Kafka topics for Gas Price Pipeline"
echo "=============================================="
echo ""

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Step 1: Checking if Kafka container is running..."
if docker ps | grep -q $KAFKA_CONTAINER; then
    echo "Kafka container is running"
else
    echo "Kafka container not found! Is it running?"
    echo "   Run 'docker ps' to see running containers"
    exit 1
fi

echo ""
echo "Step 2: Testing connection to Kafka..."
if docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP_SERVER &>/dev/null; then
    echo "Successfully connected to Kafka"
else
    echo "Cannot connect to Kafka. Trying alternative paths..."
    
    # Try to find where kafka scripts are located
    echo "   Searching for Kafka scripts in container..."
    docker exec $KAFKA_CONTAINER find / -name "kafka-topics.sh" 2>/dev/null || echo "   kafka-topics.sh not found"
    
    # Try common locations
    POSSIBLE_PATHS=(
        "/opt/kafka/bin/kafka-topics.sh"
        "/opt/kafka_2.13-3.5.0/bin/kafka-topics.sh"
        "/usr/bin/kafka-topics.sh"
        "/usr/local/bin/kafka-topics.sh"
        "/opt/bitnami/kafka/bin/kafka-topics.sh"
        "/kafka/bin/kafka-topics.sh"
    )
    
    for path in "${POSSIBLE_PATHS[@]}"; do
        if docker exec $KAFKA_CONTAINER test -f "$path" 2>/dev/null; then
            echo "   Found Kafka script at: $path"
            KAFKA_SCRIPT_PATH="$path"
            break
        fi
    done
    
    if [ -z "$KAFKA_SCRIPT_PATH" ]; then
        echo "   Could not find kafka-topics.sh anywhere"
        exit 1
    fi
fi

echo ""
echo "Step 3: Checking if topic already exists..."
EXISTING_TOPICS=$(docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER 2>/dev/null)
echo "   Existing topics: $EXISTING_TOPICS"

if echo "$EXISTING_TOPICS" | grep -q "gas-prices"; then
    echo "   Topic 'gas-prices' already exists"
else
    echo "   Topic 'gas-prices' does not exist yet"
fi

echo ""
echo "Step 4: Creating topic..."
echo "   Running command:"
echo "   docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \\"
echo "       --create \\"
echo "       --topic gas-prices \\"
echo "       --bootstrap-server $BOOTSTRAP_SERVER \\"
echo "       --partitions 3 \\"
echo "       --replication-factor 1 \\"
echo "       --if-not-exists"
echo ""

# Actually create the topic
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic gas-prices \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

RESULT=$?
if [ $RESULT -eq 0 ]; then
    echo "Topic creation command executed successfully"
else
    echo "Topic creation failed with exit code $RESULT"
fi

echo ""
echo "Step 5: Verifying topic was created..."
echo "   Listing all topics:"
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
    --list \
    --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "Step 6: Getting topic details..."
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --topic gas-prices \
    --bootstrap-server $BOOTSTRAP_SERVER 2>/dev/null || echo "   Could not describe topic (might not exist)"

echo ""
echo "=============================================="
echo "Debug complete!"
echo "=============================================="