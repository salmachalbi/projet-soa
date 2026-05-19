const { Kafka } = require('kafkajs');

class KafkaProducer {
  constructor(clientId = 'soa-producer') {
    this.kafka = new Kafka({
      clientId: clientId,
      brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
      connectionTimeout: 10000,
      requestTimeout: 30000,
    });

    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      compression: 1, // Gzip compression
    });

    this.isConnected = false;
  }

  async connect() {
    try {
      await this.producer.connect();
      this.isConnected = true;
      console.log('✅ Kafka Producer connected');
    } catch (error) {
      console.error('❌ Failed to connect Kafka Producer:', error);
      throw error;
    }
  }

  async sendMessage(topic, messages) {
    if (!this.isConnected) {
      throw new Error('Producer not connected. Call connect() first.');
    }

    try {
      const result = await this.producer.send({
        topic: topic,
        messages: Array.isArray(messages) ? messages : [messages],
        timeout: 30000,
        compression: 1,
      });

      console.log(`📤 Message(s) sent to topic "${topic}":`, result);
      return result;
    } catch (error) {
      console.error(`❌ Error sending message to topic "${topic}":`, error);
      throw error;
    }
  }

  async sendMessageWithKey(topic, key, value) {
    return this.sendMessage(topic, {
      key: String(key),
      value: JSON.stringify(value),
    });
  }

  async disconnect() {
    try {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('✅ Kafka Producer disconnected');
    } catch (error) {
      console.error('❌ Error disconnecting producer:', error);
      throw error;
    }
  }
}

module.exports = KafkaProducer;
