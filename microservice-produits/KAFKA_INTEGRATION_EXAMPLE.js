// EXAMPLE: Integration of Kafka with Microservice Produits
// Add this to your server.js file to enable Kafka event publishing

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const initDB = require('./db');
const ProductKafkaService = require('./kafkaService'); // NEW: Import Kafka Service

// Initialize Kafka Service
const kafkaService = new ProductKafkaService();

// Load Proto...
const PROTO_PATH = './product.proto';
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const productProto = grpc.loadPackageDefinition(packageDef).product;

// Base RxDB
let db;

// Initialize when server starts
(async () => {
  try {
    console.log('🔄 Initializing database and Kafka...');
    db = await initDB();
    await kafkaService.initialize();
    console.log('✅ Database and Kafka initialized successfully');

    // Optional: Listen to client events
    kafkaService.listenToClientEvents((message) => {
      console.log('📨 Client event received in Products service:', message);
      // Handle client events
    });

    // Optional: Listen to facture events
    kafkaService.listenToFactureEvents((message) => {
      console.log('📨 Facture event received in Products service:', message);
      // Handle facture events (e.g., product sold)
    });

  } catch (error) {
    console.error('❌ Initialization error:', error);
  }
})();

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('🛑 Shutting down Kafka and database...');
  try {
    await kafkaService.disconnect();
    console.log('✅ Kafka and database disconnected');
  } catch (error) {
    console.error('❌ Error disconnecting:', error);
  }
});

// Service implementation
const productService = {
  CreateProduct: async (call, callback) => {
    const p = call.request.product;
    if (!p || !p.id || !p.name) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: "id et name sont obligatoires"
      });
    }

    try {
      await db.products.insert(p);
      
      // 📤 PUBLISH EVENT TO KAFKA
      try {
        await kafkaService.publishProductCreated(p);
        console.log('✅ PRODUCT_CREATED event published');
      } catch (kafkaError) {
        console.error('❌ Failed to publish event:', kafkaError);
      }
      
      callback(null, { product: p });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetProduct: async (call, callback) => {
    try {
      const doc = await db.products.findOne(call.request.id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: "Produit introuvable" });
      callback(null, { product: doc.toJSON() });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  UpdateProduct: async (call, callback) => {
    const p = call.request.product;
    if (!p || !p.id) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: "id est obligatoire"
      });
    }

    try {
      const doc = await db.products.findOne(p.id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: "Produit introuvable" });
      
      await doc.update({ $set: p });
      
      // 📤 PUBLISH EVENT TO KAFKA
      try {
        await kafkaService.publishProductUpdated(p);
        console.log('✅ PRODUCT_UPDATED event published');
      } catch (kafkaError) {
        console.error('❌ Failed to publish event:', kafkaError);
      }
      
      callback(null, { product: p });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteProduct: async (call, callback) => {
    const id = call.request.id;
    if (!id) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: "id est obligatoire"
      });
    }

    try {
      const doc = await db.products.findOne(id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: "Produit introuvable" });
      
      await doc.remove();
      
      // 📤 PUBLISH EVENT TO KAFKA
      try {
        await kafkaService.publishProductDeleted(id);
        console.log('✅ PRODUCT_DELETED event published');
      } catch (kafkaError) {
        console.error('❌ Failed to publish event:', kafkaError);
      }
      
      callback(null, { success: true });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  // ... other methods unchanged ...
};

// Start gRPC server
const server = new grpc.Server();
server.addService(productProto.ProductService.service, productService);

const PORT = 50053;
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) throw err;
  console.log(`🚀 Products gRPC Server running on port ${port}`);
  console.log(`📡 Kafka events enabled`);
  server.start();
});
