#!/bin/bash

# Test different batch sizes
test_batch_size() {
    local batch_size=$1
    local duration=$2  # test duration in seconds
    local port=$3

    echo "Testing with batch_size=$batch_size"
    
    # Start server with specified batch size
    ./run_server.sh -p $port -b $batch_size -c "127.0.0.1:$port:0" &
    server_pid=$!
    
    # Wait for server to start
    sleep 2
    
    # Start client and send requests for specified duration
    start_time=$(date +%s)
    count=0
    
    while true; do
        current_time=$(date +%s)
        if [ $((current_time - start_time)) -ge $duration ]; then
            break
        fi
        
        ./run_client.sh -a "127.0.0.1:$port"
        if [ $? -eq 0 ]; then
            count=$((count + 1))
        fi
    done
    
    # Calculate QPS
    qps=$(echo "scale=2; $count / $duration" | bc)
    echo "Batch size: $batch_size, Total requests: $count, Duration: $duration seconds, QPS: $qps"
    
    # Stop server
    kill $server_pid
    wait $server_pid 2>/dev/null
    sleep 1
}

# Test different batch sizes
for batch_size in 100 200 400 800; do
    test_batch_size $batch_size 30 8100
    sleep 2  # Wait between tests
done 