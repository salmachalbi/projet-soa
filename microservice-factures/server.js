const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const crypto = require('crypto');
const db = require('./db');
const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

// ── gRPC setup ──────────────────────────────────────────────
const packageDef = protoLoader.loadSync('./facture.proto', {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const factureProto = grpc.loadPackageDefinition(packageDef).facture;

// ── SQL statements ──────────────────────────────────────────
const stmtInsert    = db.prepare('INSERT INTO factures (id, clientId, productId, quantity, total, date, status) VALUES (?, ?, ?, ?, ?, ?, ?)');
const stmtSelectById= db.prepare('SELECT * FROM factures WHERE id = ?');
const stmtListAll   = db.prepare('SELECT * FROM factures');
const stmtDelete    = db.prepare('DELETE FROM factures WHERE id = ?');
const stmtByClient  = db.prepare('SELECT * FROM factures WHERE clientId = ?');
const stmtByStatus  = db.prepare('SELECT * FROM factures WHERE status = ?');

// ── Kafka setup ─────────────────────────────────────────────
const producer = new KafkaProducer('factures-producer');
const consumer = new KafkaConsumer('factures-group', 'factures-consumer');

async function initKafka() {
  try {
    await producer.connect();
    await consumer.connect();

    // Écoute les événements clients (ex: client supprimé)
    await consumer.subscribe('clients-events');
    await consumer.run(async ({ topic, value }) => {
      if (value.event === 'CLIENT_DELETED') {
        console.log(`[Factures] Client ${value.data.id} supprimé - info reçue`);
      }
    });

    console.log('✓ Kafka initialisé (Factures)');
  } catch (err) {
    console.error('⚠ Kafka non disponible, le service continue sans Kafka:', err.message);
  }
}

async function publishEvent(event, key, data) {
  try {
    await producer.sendMessageWithKey('factures-events', key, {
      event,
      timestamp: new Date().toISOString(),
      data,
    });
  } catch (err) {
    console.error(`⚠ Kafka publish failed (${event}):`, err.message);
  }
}

// ── gRPC service implementation ─────────────────────────────
const factureService = {

  CreateFacture: async (call, callback) => {
    const f = call.request.facture;
    if (!f || !f.clientId || !f.productId) {
      return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'clientId et productId sont obligatoires' });
    }
    try {
      const id = f.id && f.id.trim() !== '' ? f.id : crypto.randomUUID();
      const status = f.status || 'EN_ATTENTE';
      stmtInsert.run(id, f.clientId, f.productId, f.quantity, f.total, f.date, status);
      const facture = { ...f, id, status };

      // ✅ SCÉNARIO MÉTIER : publier l'événement → produits va décrémenter le stock
      await publishEvent('FACTURE_CREATED', id, facture);
      console.log(`✓ Event FACTURE_CREATED publié → microservice produits va décrémenter le stock`);

      callback(null, { facture });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetFacture: (call, callback) => {
    try {
      const row = stmtSelectById.get(call.request.id);
      if (!row) return callback({ code: grpc.status.NOT_FOUND, message: 'Facture introuvable' });
      callback(null, { facture: row });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  ListFactures: (call, callback) => {
    try {
      callback(null, { factures: stmtListAll.all() });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetFacturesByClient: (call, callback) => {
    try {
      callback(null, { factures: stmtByClient.all(call.request.clientId) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetFacturesByStatus: (call, callback) => {
    try {
      callback(null, { factures: stmtByStatus.all(call.request.status) });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteFacture: async (call, callback) => {
    try {
      const row = stmtSelectById.get(call.request.id);
      if (!row) return callback({ code: grpc.status.NOT_FOUND, message: 'Facture introuvable' });
      stmtDelete.run(call.request.id);
      await publishEvent('FACTURE_DELETED', call.request.id, { id: call.request.id, clientId: row.clientId });
      callback(null, { success: true });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

// ── Start ────────────────────────────────────────────────────
async function main() {
  await initKafka();

  const server = new grpc.Server();
  server.addService(factureProto.FactureService.service, factureService);

  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`✓ Microservice Factures démarré sur le port ${port}`);
    console.log('✓ Base de données : SQLite3');
    console.log('✓ Kafka : publie factures-events → décrémentation stock automatique');
  });

  process.on('SIGTERM', async () => {
    await producer.disconnect();
    await consumer.disconnect();
  });
}

main();