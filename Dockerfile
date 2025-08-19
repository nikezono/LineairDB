# Dockerfile for LineairDB Linux build
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libboost-all-dev \
    libjemalloc-dev \
    libnuma-dev \
    doxygen \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Create build directory
RUN mkdir -p build

# Build the project
WORKDIR /app/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release
RUN make -j$(nproc)