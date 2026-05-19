const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, 'clients.db');

// Choisis UNE seule version :
const db = new Database(DB_PATH);

db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS clients (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    phone      TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_email ON clients(email);
  CREATE INDEX IF NOT EXISTS idx_name ON clients(name);
`);

console.log('[DB] Base de données SQLite3 initialisée :', DB_PATH);

module.exports = db;
