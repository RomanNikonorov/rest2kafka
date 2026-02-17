# REST to Kafka Producer

This project provides a simple REST API that accepts messages and forwards them to a specified Kafka topic. It's designed to work as a bridge between HTTP-based applications and Kafka, enabling easy message queuing and processing without direct Kafka client integration.

## Features

- **Simple REST API**: Send messages to Kafka topics through HTTP requests.
- **SASL Authentication**: Supports SASL authentication for secure connections to Kafka.
- **Configurable Encryption**: Offers the choice between SHA-256 and SHA-512 encryption for SASL.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Go 1.15 or higher
- Access to a Kafka cluster with or without SASL authentication

## Installation

### Option 1: Using Go

Clone the repository and build from source:

```bash
git clone https://github.com/RomanNikonorov/rest2kafka.git
cd rest2kafka
go build -o rest2kafka .
```

Run the application:

```bash
# Basic usage (without SASL authentication)
./rest2kafka -brokers=localhost:9092

# With SASL authentication
./rest2kafka -brokers=localhost:9092 -username=your_username -passwd=your_password -encription=256
```

### Option 2: Using Docker

Build the Docker image:

```bash
docker build -t rest2kafka .
```

Run the container:

```bash
# Basic usage (without SASL authentication)
docker run -d -p 8080:8080 rest2kafka -brokers=kafka-broker:9092

# With SASL authentication (SHA-256)
docker run -d -p 8080:8080 rest2kafka \
  -brokers=kafka-broker:9092 \
  -username=your_username \
  -passwd=your_password \
  -encription=256

# With SASL authentication (SHA-512)
docker run -d -p 8080:8080 rest2kafka \
  -brokers=kafka-broker:9092 \
  -username=your_username \
  -passwd=your_password \
  -encription=512
```

### Option 3: Using Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  rest2kafka:
    build: .
    ports:
      - "8080:8080"
    command: ["-brokers=kafka:9092"]
    depends_on:
      - kafka
    networks:
      - kafka-network

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - kafka-network

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - kafka-network

networks:
  kafka-network:
    driver: bridge
```

Run with Docker Compose:

```bash
docker-compose up -d
```

For SASL authentication with Docker Compose, modify the `rest2kafka` service command:

```yaml
services:
  rest2kafka:
    build: .
    ports:
      - "8080:8080"
    command: 
      - "-brokers=kafka:9092"
      - "-username=your_username"
      - "-passwd=your_password"
      - "-encription=256"
    depends_on:
      - kafka
    networks:
      - kafka-network
```

## Configuration

The application accepts the following command-line flags:

- `-brokers`: Comma-separated list of Kafka broker addresses (default: `localhost:9092`)
- `-username`: SASL username (optional, required for SASL authentication)
- `-passwd`: SASL password (optional, required for SASL authentication)
- `-encription`: Encryption mode for SASL - either `256` (SHA-256) or `512` (SHA-512) (default: `256`)

## Usage

Send messages to Kafka through the REST API:

```bash
# Send a single message
curl -X POST http://localhost:8080/send \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-topic",
    "messages": [
      {
        "message": {"text": "Hello Kafka!"},
        "header": "optional-header",
        "key": "message-key"
      }
    ]
  }'

# Send multiple messages
curl -X POST http://localhost:8080/send \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-topic",
    "messages": [
      {
        "message": {"text": "First message"},
        "key": "key1"
      },
      {
        "message": {"text": "Second message"},
        "key": "key2"
      }
    ]
  }'
```