FROM rust:latest

# Install required build tools and libraries
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    librdkafka-dev \
    protobuf-compiler \
    git \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /workspace

# Copy only Cargo.toml files first for caching
COPY ../kionas-lib/Cargo.toml ./kionas-lib/Cargo.toml
COPY ../server/Cargo.toml ./server/Cargo.toml
COPY ../worker/Cargo.toml ./worker/Cargo.toml
COPY ../client/Cargo.toml ./client/Cargo.toml
COPY ../Cargo.toml ./Cargo.toml

# Create source directories for build context
RUN mkdir -p kionas-lib/src server/src worker/src client/src

# Pre-build dependencies
RUN cargo fetch

# Copy the rest of the source code
COPY .. .

# Default command
CMD ["bash"]
