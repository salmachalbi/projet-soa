const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const initDB = require('./db');
const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

// ── gRPC setup ──────────────────────────────────────────────
const packageDef = protoLoader.loadSync('./product.proto', {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const productProto = grpc.loadPackageDefinition(packageDef).product;

let db;

// ── Kafka setup ─────────────────────────────────────────────
const producer = new KafkaProducer('products-producer');
const consumer = new KafkaConsumer('products-group', 'products-consumer');

async function initKafka() {
  try {
    await producer.connect();
    await consumer.connect();

    //  SCÉNARIO MÉTIER : quand une facture est créée → décrémenter le stock
    await consumer.subscribe('factures-events');
    await consumer.run(async ({ topic, value }) => {
      if (value.event === 'FACTURE_CREATED') {
        const { productId, quantity } = value.data;
        console.log(`[Produits] Facture créée → décrémentation stock produit ${productId} de ${quantity}`);
        try {
          const doc = await db.products.findOne(productId).exec();
          if (doc) {
            const newStock = Math.max(0, doc.stock - quantity);
            await doc.incrementalModify(old => ({ ...old, stock: newStock }));
            console.log(`✓ Stock produit ${productId} mis à jour : ${doc.stock} → ${newStock}`);
          }
        } catch (err) {
          console.error('⚠ Erreur mise à jour stock:', err.message);
        }
      }
    });

    console.log('✓ Kafka initialisé (Produits) - écoute factures-events');
  } catch (err) {
    console.error('⚠ Kafka non disponible, le service continue sans Kafka:', err.message);
  }
}

async function publishEvent(event, key, data) {
  try {
    await producer.sendMessageWithKey('products-events', key, {
      event,
      timestamp: new Date().toISOString(),
      data,
    });
  } catch (err) {
    console.error(`⚠ Kafka publish failed (${event}):`, err.message);
  }
}

// ── gRPC service implementation ─────────────────────────────
const productService = {

  CreateProduct: async (call, callback) => {
    const p = call.request.product;
    if (!p || !p.id || !p.name) return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'id et name sont obligatoires' });
    try {
      await db.products.insert(p);
      await publishEvent('PRODUCT_CREATED', p.id, p);
      callback(null, { product: p });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetProduct: async (call, callback) => {
    try {
      const doc = await db.products.findOne(call.request.id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: 'Produit introuvable' });
      callback(null, { product: doc.toJSON() });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  UpdateProduct: async (call, callback) => {
    const p = call.request.product;
    if (!p || !p.id) return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'id est obligatoire' });
    try {
      const doc = await db.products.findOne(p.id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: 'Produit introuvable' });
      await doc.incrementalModify(old => ({ ...old, ...p }));
      await publishEvent('PRODUCT_UPDATED', p.id, p);
      callback(null, { product: { ...doc.toJSON(), ...p } });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteProduct: async (call, callback) => {
    try {
      const doc = await db.products.findOne(call.request.id).exec();
      if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: 'Produit introuvable' });
      await doc.remove();
      await publishEvent('PRODUCT_DELETED', call.request.id, { id: call.request.id });
      callback(null, { success: true });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  ListProducts: async (call, callback) => {
    try {
      const docs = await db.products.find().exec();
      callback(null, { products: docs.map(d => d.toJSON()) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  SearchProductByName: async (call, callback) => {
    try {
      const docs = await db.products.find({ selector: { name: { $regex: call.request.name } } }).exec();
      callback(null, { products: docs.map(d => d.toJSON()) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetProductsByCategory: async (call, callback) => {
    try {
      const docs = await db.products.find({ selector: { category: call.request.category } }).exec();
      callback(null, { products: docs.map(d => d.toJSON()) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetProductsOutOfStock: async (call, callback) => {
    try {
      const docs = await db.products.find({ selector: { stock: { $eq: 0 } } }).exec();
      callback(null, { products: docs.map(d => d.toJSON()) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

// ── Start ────────────────────────────────────────────────────
async function main() {
  db = await initDB();
  await initKafka();

  const server = new grpc.Server();
  server.addService(productProto.ProductService.service, productService);

  server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`✓ Microservice Produits démarré sur le port ${port}`);
    console.log('✓ Base de données : RxDB (NoSQL)');
    console.log('✓ Kafka : écoute factures-events → décrémentation stock automatique');
  });

  process.on('SIGTERM', async () => {
    await producer.disconnect();
    await consumer.disconnect();
  });
}

main();