const Database = require('better-sqlite3');
const path = require('path');

//  Création/connexion à la base SQLite
const DB_PATH = path.join(__dirname, 'factures.db');
const db = new Database(DB_PATH);

//  Création de la table Factures si elle n’existe pas
db.prepare(`
  CREATE TABLE IF NOT EXISTS factures (
    id TEXT PRIMARY KEY,
    clientId TEXT NOT NULL,
    productId TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    total REAL NOT NULL,
    date TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL DEFAULT 'en attente'
  )
`).run();

console.log("✓ Base Factures initialisée:", DB_PATH);

module.exports = db;
