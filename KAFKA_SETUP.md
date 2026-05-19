# Kafka Setup for SOA Microservices

## Overview
This Kafka setup enables event-driven communication between 3 microservices:
- **Microservice Clients** - publishes client events
- **Microservice Factures** - publishes facture events
- **Microservice Produits** - publishes product events

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Kafka Broker (Docker)                      │
│                    localhost:9092                             │
└──────┬──────────────┬──────────────┬──────────────────────────┘
       │              │              │
       ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Clients    │ │  Factures    │ │   Produits   │
│ Microservice │ │ Microservice │ │ Microservice │
└──────────────┘ └──────────────┘ └──────────────┘
       │              │              │
       └──────────────┼──────────────┘
              Topics:
    - clients-events
    - factures-events
    - products-events
```

## Topics

- **clients-events**: Events related to client operations (CREATE, UPDATE, DELETE)
- **factures-events**: Events related to facture/invoice operations (CREATE, UPDATE, DELETE)
- **products-events**: Events related to product operations (CREATE, UPDATE, DELETE)

## Prerequisites

- Docker & Docker Compose installed
- Node.js 14+ installed

## Installation & Setup

### 1. Start Kafka Broker

```bash
# Navigate to project root
cd /path/to/projet-soa

# Start Kafka and Zookeeper
docker-compose up -d

# Verify Kafka is running
docker-compose logs -f kafka
```

### 2. Install Dependencies

In each microservice directory, install kafkajs:

```bash
# Microservice Clients
cd microservice-clients
npm install

# Microservice Factures
cd ../microservice-factures
npm install

# Microservice Produits
cd ../microservice-produits
npm install
```

### 3. Check Kafka Status

```bash
# Check container status
docker-compose ps

# View Kafka logs
docker-compose logs kafka

# Stop Kafka
docker-compose down

# Remove volumes (optional - deletes Kafka data)
docker-compose down -v
```

## Integration Guide

### Using KafkaProducer

```javascript
const KafkaProducer = require('../kafka/kafkaProducer');

const producer = new KafkaProducer('my-producer');
await producer.connect();

// Send message with key
await producer.sendMessageWithKey('topic-name', 'key', {
  event: 'EVENT_NAME',
  data: { /* your data */ }
});

await producer.disconnect();
```

### Using KafkaConsumer

```javascript
const KafkaConsumer = require('../kafka/kafkaConsumer');

const consumer = new KafkaConsumer('my-group');
await consumer.connect();
await consumer.subscribe('topic-name');

await consumer.run(async (message) => {
  console.log('Received:', message);
  // Process message
});
```

### Using Service-Specific Kafka Services

Each microservice has a `kafkaService.js` file:

```javascript
const ClientKafkaService = require('./kafkaService');

const kafkaService = new ClientKafkaService();
await kafkaService.initialize();

// Publish events
await kafkaService.publishClientCreated(clientData);
await kafkaService.publishClientUpdated(clientData);
await kafkaService.publishClientDeleted(clientId);

// Listen to other services' events
await kafkaService.listenToProductEvents((message) => {
  console.log('Product event received:', message);
});

await kafkaService.disconnect();
```

## Integration with gRPC Servers

### Example: Microservice Clients

```javascript
const ClientKafkaService = require('./kafkaService');
const kafkaService = new ClientKafkaService();

// Initialize Kafka when server starts
(async () => {
  try {
    await kafkaService.initialize();
    
    // Listen to facture events
    await kafkaService.listenToFactureEvents((msg) => {
      if (msg.value.event === 'FACTURE_CREATED') {
        console.log('New facture for client:', msg.value.data.clientId);
      }
    });
  } catch (error) {
    console.error('Kafka error:', error);
  }
})();

const clientService = {
  CreateClient: async (call, callback) => {
    // ... existing code ...
    
    // Publish Kafka event
    await kafkaService.publishClientCreated(client);
    
    callback(null, { client });
  },
  
  // Add similar event publishing to Update and Delete operations
};
```

## Environment Variables

Create `.env` file in project root:

```env
KAFKA_BROKER=localhost:9092
NODE_ENV=development
```

## Monitoring & Troubleshooting

### Check Kafka Topics

```bash
# Inside Kafka container
docker exec -it <kafka-container-id> kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec -it <kafka-container-id> kafka-topics --bootstrap-server localhost:9092 --describe --topic clients-events

# Consume messages from topic
docker exec -it <kafka-container-id> kafka-console-consumer --bootstrap-server localhost:9092 --topic clients-events --from-beginning
```

### Common Issues

1. **Connection Refused**: Ensure Kafka is running
   ```bash
   docker-compose ps
   ```

2. **Topic Auto-Creation**: Enabled by default in docker-compose.yml
   ```yaml
   KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
   ```

3. **Message Format**: Ensure messages are JSON-serializable
   ```javascript
   await producer.sendMessageWithKey('topic', key, JSON.stringify(data));
   ```

## Next Steps

1. Integrate Kafka event publishing into each microservice's RPC handlers
2. Set up consumers to react to events from other services
3. Add error handling and retry logic
4. Implement message persistence strategies
5. Add monitoring/metrics collection

## Resources

- [KafkaJS Documentation](https://kafka.js.org/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Compose](https://docs.docker.com/compose/)
