const express = require('express');
const { ApolloServer } = require('apollo-server-express');
const resolvers = require('./resolvers');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const typeDefs = fs.readFileSync(path.join(__dirname, 'schema.gql'), 'utf8');

const server = new ApolloServer({ typeDefs, resolvers });
async function startApollo() {
  await server.start();
  server.applyMiddleware({ app, path: '/graphql' });
}
startApollo();

/* ---------------- REST Clients ---------------- */

app.get('/clients/search', (req, res) => {
  resolvers.Query.searchClientByName(null, { name: req.query.name })
    .then(clients => res.json(clients))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/clients', (req, res) => {
  resolvers.Query.listClients()
    .then(clients => res.json(clients))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.post('/clients', (req, res) => {
  resolvers.Mutation.createClient(null, req.body)
    .then(client => res.json(client))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/clients/:id', (req, res) => {
  resolvers.Query.client(null, { id: req.params.id })
    .then(client => res.json(client))
    .catch(err => res.status(404).json({ error: err.message }));
});

app.put('/clients/:id', (req, res) => {
  resolvers.Mutation.updateClient(null, { id: req.params.id, ...req.body })
    .then(client => res.json(client))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.delete('/clients/:id', (req, res) => {
  resolvers.Mutation.deleteClient(null, { id: req.params.id })
    .then(success => res.json({ success }))
    .catch(err => res.status(500).json({ error: err.message }));
});

/* ---------------- REST Produits ---------------- */

app.get('/products/search', (req, res) => {
  resolvers.Query.searchProductByName(null, { name: req.query.name })
    .then(products => res.json(products))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/products/category', (req, res) => {
  resolvers.Query.getProductsByCategory(null, { category: req.query.category })
    .then(products => res.json(products))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/products/outofstock', (req, res) => {
  resolvers.Query.getProductsOutOfStock(null, {})
    .then(products => res.json(products))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/products', (req, res) => {
  resolvers.Query.listProducts
    ? resolvers.Query.listProducts()
        .then(products => res.json(products))
        .catch(err => res.status(500).json({ error: err.message }))
    : res.status(501).json({ error: 'Not implemented' });
});

app.post('/products', (req, res) => {
  resolvers.Mutation.createProduct(null, req.body)
    .then(product => res.json(product))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/products/:id', (req, res) => {
  resolvers.Query.product(null, { id: req.params.id })
    .then(product => res.json(product))
    .catch(err => res.status(404).json({ error: err.message }));
});

app.put('/products/:id', (req, res) => {
  resolvers.Mutation.updateProduct(null, { id: req.params.id, ...req.body })
    .then(product => res.json(product))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.delete('/products/:id', (req, res) => {
  resolvers.Mutation.deleteProduct(null, { id: req.params.id })
    .then(success => res.json({ success }))
    .catch(err => res.status(500).json({ error: err.message }));
});

/* ---------------- REST Factures ---------------- */

app.get('/factures/status', (req, res) => {
  resolvers.Query.getFacturesByStatus(null, { status: req.query.status })
    .then(factures => res.json(factures))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/factures/client', (req, res) => {
  resolvers.Query.getFacturesByClient(null, { clientId: req.query.clientId })
    .then(factures => res.json(factures))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/factures', (req, res) => {
  resolvers.Query.listFactures
    ? resolvers.Query.listFactures()
        .then(factures => res.json(factures))
        .catch(err => res.status(500).json({ error: err.message }))
    : res.status(501).json({ error: 'Not implemented' });
});

app.post('/factures', (req, res) => {
  resolvers.Mutation.createFacture(null, req.body)
    .then(facture => res.json(facture))
    .catch(err => res.status(500).json({ error: err.message }));
});

app.get('/factures/:id', (req, res) => {
  resolvers.Query.facture(null, { id: req.params.id })
    .then(facture => res.json(facture))
    .catch(err => res.status(404).json({ error: err.message }));
});

app.delete('/factures/:id', (req, res) => {
  resolvers.Mutation.deleteFacture(null, { id: req.params.id })
    .then(success => res.json({ success }))
    .catch(err => res.status(500).json({ error: err.message }));
});

/* ---------------- Démarrage ---------------- */

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`✓ API Gateway démarrée sur http://localhost:${PORT}`);
  console.log(`✓ REST endpoints disponibles`);
  console.log(`✓ GraphQL disponible sur http://localhost:${PORT}/graphql`);
});
