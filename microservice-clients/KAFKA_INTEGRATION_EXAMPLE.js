// EXAMPLE: Integration of Kafka with Microservice Clients
// Add this to your server.js file to enable Kafka event publishing

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const crypto = require('crypto');
const db = require('./db');
const ClientKafkaService = require('./kafkaService'); // NEW: Import Kafka Service

// Initialize Kafka Service
const kafkaService = new ClientKafkaService();

// Initialize when server starts
(async () => {
  try {
    console.log('🔄 Initializing Kafka...');
    await kafkaService.initialize();
    console.log('✅ Kafka initialized successfully');

    // Optional: Listen to facture events
    kafkaService.listenToFactureEvents((message) => {
      console.log('📨 Facture event received in Clients service:', message);
      // Handle facture events related to clients
    });

  } catch (error) {
    console.error('❌ Kafka initialization error:', error);
    // Don't crash the app, just log the error
  }
})();

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('🛑 Shutting down Kafka...');
  try {
    await kafkaService.disconnect();
    console.log('✅ Kafka disconnected');
  } catch (error) {
    console.error('❌ Error disconnecting Kafka:', error);
  }
});

// Load Proto...
const PROTO_PATH = './client.proto';
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const clientProto = grpc.loadPackageDefinition(packageDef).client;

// Database statements...
const stmtInsert = db.prepare(
  'INSERT INTO clients (id, name, email, phone) VALUES (?, ?, ?, ?)'
);
const stmtSelectById = db.prepare('SELECT * FROM clients WHERE id = ?');
const stmtUpdate = db.prepare(
  'UPDATE clients SET name = ?, email = ?, phone = ?, updated_at = datetime(\'now\') WHERE id = ?'
);
const stmtDelete = db.prepare('DELETE FROM clients WHERE id = ?');

// Service implementation
const clientService = {
  CreateClient: async (call, callback) => {
    const c = call.request.client;

    if (!c || !c.name) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'name est obligatoire',
      });
    }

    try {
      const id = c.id && c.id.trim() !== '' ? c.id : crypto.randomUUID();
      stmtInsert.run(id, c.name, c.email || '', c.phone || '');
      
      const client = { id, name: c.name, email: c.email || '', phone: c.phone || '' };
      
      // 📤 PUBLISH EVENT TO KAFKA
      try {
        await kafkaService.publishClientCreated(client);
        console.log('✅ CLIENT_CREATED event published');
      } catch (kafkaError) {
        console.error('❌ Failed to publish event:', kafkaError);
        // Continue anyway - don't fail the RPC
      }
      
      callback(null, { client });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  UpdateClient: (call, callback) => {
    const c = call.request.client;
    if (!c || !c.id) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'id est obligatoire',
      });
    }

    try {
      stmtUpdate.run(c.name, c.email || '', c.phone || '', c.id);
      
      // 📤 PUBLISH EVENT TO KAFKA
      (async () => {
        try {
          await kafkaService.publishClientUpdated(c);
          console.log('✅ CLIENT_UPDATED event published');
        } catch (kafkaError) {
          console.error('❌ Failed to publish event:', kafkaError);
        }
      })();
      
      callback(null, { client: c });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteClient: (call, callback) => {
    const id = call.request.id;
    if (!id) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'id est obligatoire',
      });
    }

    try {
      stmtDelete.run(id);
      
      // 📤 PUBLISH EVENT TO KAFKA
      (async () => {
        try {
          await kafkaService.publishClientDeleted(id);
          console.log('✅ CLIENT_DELETED event published');
        } catch (kafkaError) {
          console.error('❌ Failed to publish event:', kafkaError);
        }
      })();
      
      callback(null, { success: true });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // ... other methods unchanged ...
};

// Start gRPC server
const server = new grpc.Server();
server.addService(clientProto.ClientService.service, clientService);

const PORT = 50051;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) throw err;
  console.log(`🚀 Clients gRPC Server running on port ${port}`);
  console.log(`📡 Kafka events enabled`);
  server.start();
});
