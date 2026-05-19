const { createRxDatabase } = require('rxdb');
const { getRxStorageSQLiteTrial, getSQLiteBasicsNode } = require('rxdb/plugins/storage-sqlite');
const sqlite3 = require('sqlite3');

async function initDB() {
  const db = await createRxDatabase({
    name: 'productsdb',
    storage: getRxStorageSQLiteTrial({
      sqliteBasics: getSQLiteBasicsNode(sqlite3)
    }),
  });

  await db.addCollections({
    products: {
      schema: {
        title: 'product schema',
        version: 0,
        type: 'object',
        primaryKey: 'id',
        properties: {
          id:         { type: 'string', maxLength: 100 },
          name:       { type: 'string' },
          price:      { type: 'number' },
          category:   { type: 'string' },
          photo:      { type: 'string' },
          stock:      { type: 'number' },
          created_at: { type: 'string', format: 'date-time' },
          updated_at: { type: 'string', format: 'date-time' }
        },
        required: ['id', 'name', 'price', 'category', 'stock', 'photo']
      }
    }
  });

  console.log('✓ Base RxDB initialisée (Produits) - SQLite persistant');
  return db;
}

module.exports = initDB;