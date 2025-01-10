#!/bin/bash

# Set the path
SCRIPT_DIR=$(dirname $(readlink -f "$0"))
cd $SCRIPT_DIR

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -a|--addr)
            addr="$2"
            shift 2
            ;;
        -t|--timeout)
            timeout="$2"
            shift 2
            ;;
        -s|--size)
            size="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set default values
addr=${addr:-"127.0.0.1:8100"}
timeout=${timeout:-1000}
size=${size:-256}

# Start client
exec ./payload_test_client \
    -addr=${addr} \
    -timeout_ms=${timeout} \
    -request_size=${size} 