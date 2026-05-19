# Kafka Broker Setup - Quick Start Guide

## 📋 What Has Been Created

Your Kafka setup is now complete with:

1. **Docker Compose Configuration** (`docker-compose.yml`)
   - Kafka broker on port 9092
   - Zookeeper on port 2181
   - Auto-create topics enabled

2. **Kafka Modules**
   - `kafka/kafkaProducer.js` - Send messages
   - `kafka/kafkaConsumer.js` - Receive messages

3. **Service-Specific Kafka Integrations**
   - `microservice-clients/kafkaService.js`
   - `microservice-factures/kafkaService.js`
   - `microservice-produits/kafkaService.js`

4. **Example Integration Files**
   - `**/KAFKA_INTEGRATION_EXAMPLE.js` - Shows how to integrate

5. **Documentation**
   - `KAFKA_SETUP.md` - Comprehensive guide
   - `start-kafka.sh` / `start-kafka.bat` - Quick start scripts

## 🚀 Quick Start (3 Steps)

### Step 1: Start Kafka Broker

**Windows:**
```bash
cd c:\Users\Azuz\Downloads\projet-soa\projet-soa
start-kafka.bat
```

**Linux/Mac:**
```bash
cd /path/to/projet-soa
bash start-kafka.sh
```

Or manually:
```bash
docker-compose up -d
```

### Step 2: Install Dependencies

```bash
cd microservice-clients && npm install
cd ../microservice-factures && npm install
cd ../microservice-produits && npm install
```

### Step 3: Integrate Kafka into Your Microservices

For each microservice, add Kafka integration to `server.js`:

```javascript
const KafkaService = require('./kafkaService');
const kafkaService = new KafkaService();

// At server startup
await kafkaService.initialize();

// In your RPC handlers
await kafkaService.publishClientCreated(client); // for clients
await kafkaService.publishFactureCreated(facture); // for factures
await kafkaService.publishProductCreated(product); // for products
```

See the `KAFKA_INTEGRATION_EXAMPLE.js` files in each microservice for complete examples.

## 📚 Available Kafka Events

### Clients Microservice
- `CLIENT_CREATED` - Published when a client is created
- `CLIENT_UPDATED` - Published when a client is updated
- `CLIENT_DELETED` - Published when a client is deleted
- **Listens to**: `products-events`, `factures-events`

### Factures Microservice
- `FACTURE_CREATED` - Published when an invoice is created
- `FACTURE_UPDATED` - Published when an invoice is updated
- `FACTURE_DELETED` - Published when an invoice is deleted
- **Listens to**: `clients-events`, `products-events`

### Produits Microservice
- `PRODUCT_CREATED` - Published when a product is created
- `PRODUCT_UPDATED` - Published when a product is updated
- `PRODUCT_DELETED` - Published when a product is deleted
- **Listens to**: `clients-events`, `factures-events`

## ✅ Verify Kafka is Running

```bash
# Check containers
docker-compose ps

# View Kafka logs
docker-compose logs -f kafka

# List Kafka topics (in Kafka container)
docker exec -it <kafka-container-id> kafka-topics --bootstrap-server localhost:9092 --list
```

## 🛑 Stop Kafka

```bash
docker-compose down
```

## 💡 Common Tasks

### Publish an Event

```javascript
const KafkaProducer = require('../kafka/kafkaProducer');
const producer = new KafkaProducer('my-producer');
await producer.connect();

await producer.sendMessageWithKey('clients-events', 'client-123', {
  event: 'CLIENT_CREATED',
  data: { id: 'client-123', name: 'John Doe' }
});

await producer.disconnect();
```

### Listen to Events

```javascript
const KafkaConsumer = require('../kafka/kafkaConsumer');
const consumer = new KafkaConsumer('my-group');
await consumer.connect();
await consumer.subscribe('clients-events');

await consumer.run((message) => {
  console.log('Event:', message);
});
```

### Monitor Messages

```bash
docker exec -it <kafka-container-id> kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic clients-events \
  --from-beginning
```

## 📖 Documentation

- **Full Setup Guide**: See `KAFKA_SETUP.md`
- **Integration Examples**: See `KAFKA_INTEGRATION_EXAMPLE.js` in each microservice

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Ensure Docker and Kafka are running: `docker-compose ps` |
| Topics not created | Ensure `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'` in docker-compose.yml |
| Messages not appearing | Check consumer is subscribed to correct topic |
| Port 9092 already in use | Change port in docker-compose.yml or stop other services |

## 📊 Architecture Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Microservice   │     │  Microservice   │     │  Microservice   │
│    Clients      │     │    Factures     │     │    Produits     │
│  (gRPC Server)  │     │  (gRPC Server)  │     │  (gRPC Server)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Kafka Broker         │
                    │  localhost:9092        │
                    ├────────────────────────┤
                    │  Topics:               │
                    │  • clients-events      │
                    │  • factures-events     │
                    │  • products-events     │
                    └────────────┬───────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Zookeeper           │
                    │   localhost:2181       │
                    └────────────────────────┘
```

## 🎯 Next Steps

1. **Test Kafka Connection**: Run a simple producer/consumer test
2. **Integrate Events**: Add event publishing to each microservice's RPC handlers
3. **Event Handlers**: Implement handlers for events from other microservices
4. **Error Handling**: Add retry logic and dead-letter topics
5. **Monitoring**: Set up metrics and logging for Kafka events

Happy coding! 🚀
