FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    autoconf \
    automake \
    bison \
    flex \
    g++ \
    libboost-all-dev \
    libevent-dev \
    libssl-dev \
    libtool \
    pkg-config \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Thrift 0.22.0
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/thrift/0.22.0/thrift-0.22.0.tar.gz && \
    tar -xvf thrift-0.22.0.tar.gz && \
    cd thrift-0.22.0 && \
    ./configure --without-python --without-java --without-go --without-js --without-php --without-swift && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/thrift-0.22.0 /tmp/thrift-0.22.0.tar.gz && \
    ldconfig

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"] 