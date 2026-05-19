const { Kafka } = require('kafkajs');

class KafkaConsumer {
  constructor(groupId, clientId = 'soa-consumer') {
    this.kafka = new Kafka({
      clientId: clientId,
      brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
      connectionTimeout: 10000,
      requestTimeout: 30000,
    });

    this.consumer = this.kafka.consumer({
      groupId: groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    this.isConnected = false;
  }

  async connect() {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      console.log(`✅ Kafka Consumer connected (Group: ${this.consumer.group})`);
    } catch (error) {
      console.error('❌ Failed to connect Kafka Consumer:', error);
      throw error;
    }
  }

  async subscribe(topics) {
    if (!this.isConnected) {
      throw new Error('Consumer not connected. Call connect() first.');
    }

    try {
      const topicsArray = Array.isArray(topics) ? topics : [topics];
      await this.consumer.subscribe({
        topics: topicsArray,
        fromBeginning: false,
      });
      console.log(`📥 Subscribed to topics: ${topicsArray.join(', ')}`);
    } catch (error) {
      console.error('❌ Error subscribing to topics:', error);
      throw error;
    }
  }

  async run(messageHandler) {
    try {
      await this.consumer.run({
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 10,
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const value = JSON.parse(message.value.toString());
            const key = message.key ? message.key.toString() : null;

            console.log(`📨 Message received from topic "${topic}":`, {
              key,
              value,
              partition,
              offset: message.offset,
            });

            if (messageHandler) {
              await messageHandler({
                topic,
                key,
                value,
                partition,
                offset: message.offset,
              });
            }
          } catch (error) {
            console.error(`❌ Error processing message from topic "${topic}":`, error);
          }
        },
      });
    } catch (error) {
      console.error('❌ Error running consumer:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
      this.isConnected = false;
      console.log('✅ Kafka Consumer disconnected');
    } catch (error) {
      console.error('❌ Error disconnecting consumer:', error);
      throw error;
    }
  }
}

module.exports = KafkaConsumer;
