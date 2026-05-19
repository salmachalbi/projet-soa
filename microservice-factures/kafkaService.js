const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

class FactureKafkaService {
  constructor() {
    this.producer = new KafkaProducer('factures-producer');
    this.consumer = new KafkaConsumer('factures-group', 'factures-consumer');
  }

  async initialize() {
    await this.producer.connect();
    await this.consumer.connect();
  }

  // Publish facture events
  async publishFactureCreated(facture) {
    return this.producer.sendMessageWithKey('factures-events', facture.id, {
      event: 'FACTURE_CREATED',
      timestamp: new Date().toISOString(),
      data: facture,
    });
  }

  async publishFactureUpdated(facture) {
    return this.producer.sendMessageWithKey('factures-events', facture.id, {
      event: 'FACTURE_UPDATED',
      timestamp: new Date().toISOString(),
      data: facture,
    });
  }

  async publishFactureDeleted(factureId) {
    return this.producer.sendMessageWithKey('factures-events', factureId, {
      event: 'FACTURE_DELETED',
      timestamp: new Date().toISOString(),
      data: { id: factureId },
    });
  }

  // Listen to events from other services
  async listenToClientEvents(handler) {
    await this.consumer.subscribe('clients-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Factures service received client event:', message);
      if (handler) await handler(message);
    });
  }

  async listenToProductEvents(handler) {
    await this.consumer.subscribe('products-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Factures service received product event:', message);
      if (handler) await handler(message);
    });
  }

  async disconnect() {
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }
}

module.exports = FactureKafkaService;
