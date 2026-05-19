const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const crypto = require('crypto');
const db = require('./db');
const KafkaProducer = require('../kafka/kafkaProducer');
const KafkaConsumer = require('../kafka/kafkaConsumer');

// ── gRPC setup ──────────────────────────────────────────────
const packageDef = protoLoader.loadSync('./client.proto', {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const clientProto = grpc.loadPackageDefinition(packageDef).client;

// ── SQL statements ──────────────────────────────────────────
const stmtInsert      = db.prepare('INSERT INTO clients (id, name, email, phone) VALUES (?, ?, ?, ?)');
const stmtSelectById  = db.prepare('SELECT * FROM clients WHERE id = ?');
const stmtUpdate      = db.prepare("UPDATE clients SET name=?, email=?, phone=?, updated_at=datetime('now') WHERE id=?");
const stmtDelete      = db.prepare('DELETE FROM clients WHERE id = ?');
const stmtSearchByName= db.prepare('SELECT * FROM clients WHERE name LIKE ?');
const stmtListAll     = db.prepare('SELECT * FROM clients');

// ── Kafka setup ─────────────────────────────────────────────
const producer = new KafkaProducer('clients-producer');
const consumer = new KafkaConsumer('clients-group', 'clients-consumer');

async function initKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe('factures-events');
    await consumer.run(async ({ topic, value }) => {
      // Scénario métier : si une facture est supprimée, log côté clients
      if (value.event === 'FACTURE_DELETED') {
        console.log(`[Clients] Facture supprimée pour client ${value.data.clientId}`);
      }
    });
    console.log('✓ Kafka initialisé (Clients)');
  } catch (err) {
    console.error('⚠ Kafka non disponible, le service continue sans Kafka:', err.message);
  }
}

async function publishEvent(event, key, data) {
  try {
    await producer.sendMessageWithKey('clients-events', key, {
      event,
      timestamp: new Date().toISOString(),
      data,
    });
  } catch (err) {
    console.error(`⚠ Kafka publish failed (${event}):`, err.message);
  }
}

// ── gRPC service implementation ─────────────────────────────
const clientService = {

  CreateClient: async (call, callback) => {
    const c = call.request.client;
    if (!c || !c.name) return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'name est obligatoire' });
    try {
      const id = c.id && c.id.trim() !== '' ? c.id : crypto.randomUUID();
      stmtInsert.run(id, c.name, c.email || '', c.phone || '');
      const client = { id, name: c.name, email: c.email || '', phone: c.phone || '' };
      await publishEvent('CLIENT_CREATED', id, client);
      callback(null, { client });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  GetClient: (call, callback) => {
    try {
      const row = stmtSelectById.get(call.request.id);
      if (!row) return callback({ code: grpc.status.NOT_FOUND, message: 'Client introuvable' });
      callback(null, { client: row });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  UpdateClient: async (call, callback) => {
    const c = call.request.client;
    if (!c || !c.id) return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'id est obligatoire' });
    try {
      const result = stmtUpdate.run(c.name, c.email, c.phone, c.id);
      if (result.changes === 0) return callback({ code: grpc.status.NOT_FOUND, message: 'Client introuvable' });
      await publishEvent('CLIENT_UPDATED', c.id, c);
      callback(null, { client: c });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  DeleteClient: async (call, callback) => {
    try {
      const result = stmtDelete.run(call.request.id);
      if (result.changes === 0) return callback({ code: grpc.status.NOT_FOUND, message: 'Client introuvable' });
      await publishEvent('CLIENT_DELETED', call.request.id, { id: call.request.id });
      callback(null, { success: true });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  SearchClientByName: (call, callback) => {
    try {
      const rows = stmtSearchByName.all(`%${call.request.name}%`);
      callback(null, { clients: rows });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  ListClients: (call, callback) => {
    try {
      const rows = stmtListAll.all();
      callback(null, { clients: rows, next_page_token: '' });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

// ── Start ────────────────────────────────────────────────────
async function main() {
  await initKafka();

  const server = new grpc.Server();
  server.addService(clientProto.ClientService.service, clientService);

  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`✓ Microservice Clients démarré sur le port ${port}`);
    console.log('✓ Base de données : SQLite3');
  });

  process.on('SIGTERM', async () => {
    await producer.disconnect();
    await consumer.disconnect();
  });
}

main();