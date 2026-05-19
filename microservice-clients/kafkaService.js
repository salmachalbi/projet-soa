const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

class ClientKafkaService {
  constructor() {
    this.producer = new KafkaProducer('clients-producer');
    this.consumer = new KafkaConsumer('clients-group', 'clients-consumer');
  }

  async initialize() {
    await this.producer.connect();
    await this.consumer.connect();
  }

  // Publish client events
  async publishClientCreated(client) {
    return this.producer.sendMessageWithKey('clients-events', client.id, {
      event: 'CLIENT_CREATED',
      timestamp: new Date().toISOString(),
      data: client,
    });
  }

  async publishClientUpdated(client) {
    return this.producer.sendMessageWithKey('clients-events', client.id, {
      event: 'CLIENT_UPDATED',
      timestamp: new Date().toISOString(),
      data: client,
    });
  }

  async publishClientDeleted(clientId) {
    return this.producer.sendMessageWithKey('clients-events', clientId, {
      event: 'CLIENT_DELETED',
      timestamp: new Date().toISOString(),
      data: { id: clientId },
    });
  }

  // Listen to events from other services
  async listenToProductEvents(handler) {
    await this.consumer.subscribe('products-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Clients service received product event:', message);
      if (handler) await handler(message);
    });
  }

  async listenToFactureEvents(handler) {
    await this.consumer.subscribe('factures-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Clients service received facture event:', message);
      if (handler) await handler(message);
    });
  }

  async disconnect() {
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }
}

module.exports = ClientKafkaService;
