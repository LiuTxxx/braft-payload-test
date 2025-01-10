#!/bin/bash

# Set the path
SCRIPT_DIR=$(dirname $(readlink -f "$0"))
cd $SCRIPT_DIR

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--port)
            port="$2"
            shift 2
            ;;
        -g|--group)
            group="$2"
            shift 2
            ;;
        -d|--data)
            data="$2"
            shift 2
            ;;
        -c|--conf)
            conf="$2"
            shift 2
            ;;
        -b|--batch)
            batch="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set default values
port=${port:-8100}
group=${group:-PayloadTest}
data=${data:-"./data"}
batch=${batch:-400}

# Create data directory
mkdir -p $data

# Start server
exec ./payload_test_server \
    -port=${port} \
    -group=${group} \
    -data_path=${data} \
    -conf="${conf}" \
    -raft_leader_batch=${batch} 