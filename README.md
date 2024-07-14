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

Clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/rest2kafka.git
cd rest2kafka
```