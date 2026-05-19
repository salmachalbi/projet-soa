// EXAMPLE: Integration of Kafka with Microservice Factures
// Add this to your server.js file to enable Kafka event publishing

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const crypto = require('crypto');
const db = require('./db');
const FactureKafkaService = require('./kafkaService'); // NEW: Import Kafka Service

// Initialize Kafka Service
const kafkaService = new FactureKafkaService();

// Initialize when server starts
(async () => {
  try {
    console.log('🔄 Initializing Kafka...');
    await kafkaService.initialize();
    console.log('✅ Kafka initialized successfully');

    // Optional: Listen to client events
    kafkaService.listenToClientEvents((message) => {
      console.log('📨 Client event received in Factures service:', message);
      // Handle client events (e.g., client deleted)
    });

    // Optional: Listen to product events
    kafkaService.listenToProductEvents((message) => {
      console.log('📨 Product event received in Factures service:', message);
      // Handle product events
    });

  } catch (error) {
    console.error('❌ Kafka initialization error:', error);
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
const PROTO_PATH = './facture.proto';
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const factureProto = grpc.loadPackageDefinition(packageDef).facture;

// Database statements...
const stmtInsert = db.prepare(
  'INSERT INTO factures (id, clientId, productId, quantity, total, date, status) VALUES (?, ?, ?, ?, ?, ?, ?)'
);
const stmtSelectById = db.prepare('SELECT * FROM factures WHERE id = ?');
const stmtUpdate = db.prepare(
  'UPDATE factures SET status = ?, updated_at = datetime(\'now\') WHERE id = ?'
);
const stmtDelete = db.prepare('DELETE FROM factures WHERE id = ?');

// Service implementation
const factureService = {
  CreateFacture: async (call, callback) => {
    const f = call.request.facture;
    if (!f || !f.clientId || !f.productId) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'clientId et productId sont obligatoires',
      });
    }

    try {
      const id = f.id && f.id.trim() !== '' ? f.id : crypto.randomUUID();
      stmtInsert.run(id, f.clientId, f.productId, f.quantity, f.total, f.date, f.status || 'PENDING');
      
      const facture = { ...f, id, status: f.status || 'PENDING' };
      
      // 📤 PUBLISH EVENT TO KAFKA
      try {
        await kafkaService.publishFactureCreated(facture);
        console.log('✅ FACTURE_CREATED event published');
      } catch (kafkaError) {
        console.error('❌ Failed to publish event:', kafkaError);
      }
      
      callback(null, { facture });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  UpdateFacture: (call, callback) => {
    const f = call.request.facture;
    if (!f || !f.id) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'id est obligatoire',
      });
    }

    try {
      stmtUpdate.run(f.status || 'PENDING', f.id);
      
      // 📤 PUBLISH EVENT TO KAFKA
      (async () => {
        try {
          await kafkaService.publishFactureUpdated(f);
          console.log('✅ FACTURE_UPDATED event published');
        } catch (kafkaError) {
          console.error('❌ Failed to publish event:', kafkaError);
        }
      })();
      
      callback(null, { facture: f });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteFacture: (call, callback) => {
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
          await kafkaService.publishFactureDeleted(id);
          console.log('✅ FACTURE_DELETED event published');
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
server.addService(factureProto.FactureService.service, factureService);

const PORT = 50052;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) throw err;
  console.log(`🚀 Factures gRPC Server running on port ${port}`);
  console.log(`📡 Kafka events enabled`);
  server.start();
});
