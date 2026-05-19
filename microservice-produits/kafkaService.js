const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

class ProductKafkaService {
  constructor() {
    this.producer = new KafkaProducer('products-producer');
    this.consumer = new KafkaConsumer('products-group', 'products-consumer');
  }

  async initialize() {
    await this.producer.connect();
    await this.consumer.connect();
  }

  // Publish product events
  async publishProductCreated(product) {
    return this.producer.sendMessageWithKey('products-events', product.id, {
      event: 'PRODUCT_CREATED',
      timestamp: new Date().toISOString(),
      data: product,
    });
  }

  async publishProductUpdated(product) {
    return this.producer.sendMessageWithKey('products-events', product.id, {
      event: 'PRODUCT_UPDATED',
      timestamp: new Date().toISOString(),
      data: product,
    });
  }

  async publishProductDeleted(productId) {
    return this.producer.sendMessageWithKey('products-events', productId, {
      event: 'PRODUCT_DELETED',
      timestamp: new Date().toISOString(),
      data: { id: productId },
    });
  }

  // Listen to events from other services
  async listenToClientEvents(handler) {
    await this.consumer.subscribe('clients-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Products service received client event:', message);
      if (handler) await handler(message);
    });
  }

  async listenToFactureEvents(handler) {
    await this.consumer.subscribe('factures-events');
    await this.consumer.run(async (message) => {
      console.log('📨 Products service received facture event:', message);
      if (handler) await handler(message);
    });
  }

  async disconnect() {
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }
}

module.exports = ProductKafkaService;
