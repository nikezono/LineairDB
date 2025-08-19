#!/bin/bash

# Build and test LineairDB in Linux environment using Docker

echo "Building LineairDB Docker image..."
docker build -t lineairdb-linux .

if [ $? -ne 0 ]; then
    echo "Docker build failed!"
    exit 1
fi

echo "Running LineairDB build and tests in Linux container..."
docker run --rm -it lineairdb-linux

echo "To run an interactive shell in the container, use:"
echo "docker run --rm -it lineairdb-linux bash"
