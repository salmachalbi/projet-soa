const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

function loadProto(protoPath, pkg) {
  const packageDef = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDef)[pkg];
}

const clientProto = loadProto(path.join(__dirname, '../microservice-clients/client.proto'), 'client');
const productProto = loadProto(path.join(__dirname, '../microservice-produits/product.proto'), 'product');
const factureProto = loadProto(path.join(__dirname, '../microservice-factures/facture.proto'), 'facture');

const clientService  = new clientProto.ClientService('localhost:50051', grpc.credentials.createInsecure());
const productService = new productProto.ProductService('localhost:50052', grpc.credentials.createInsecure());
const factureService = new factureProto.FactureService('localhost:50053', grpc.credentials.createInsecure());

const resolvers = {
  Query: {
    // Clients
    client: (_, { id }) => new Promise((resolve, reject) => {
      clientService.GetClient({ id }, (err, res) => err ? reject(err) : resolve(res.client));
    }),
    listClients: () => new Promise((resolve, reject) => {
      clientService.ListClients({}, (err, res) => err ? reject(err) : resolve(res.clients));
    }),
    searchClientByName: (_, { name }) => new Promise((resolve, reject) => {
      clientService.SearchClientByName({ name }, (err, res) => err ? reject(err) : resolve(res.clients));
    }),

    // Produits
    product: (_, { id }) => new Promise((resolve, reject) => {
      productService.GetProduct({ id }, (err, res) => err ? reject(err) : resolve(res.product));
    }),
    listProducts: () => new Promise((resolve, reject) => {
      productService.ListProducts({}, (err, res) => err ? reject(err) : resolve(res.products));
    }),
    searchProductByName: (_, { name }) => new Promise((resolve, reject) => {
      productService.SearchProductByName({ name }, (err, res) => err ? reject(err) : resolve(res.products));
    }),
    getProductsByCategory: (_, { category }) => new Promise((resolve, reject) => {
      productService.GetProductsByCategory({ category }, (err, res) => err ? reject(err) : resolve(res.products));
    }),
    getProductsOutOfStock: () => new Promise((resolve, reject) => {
      productService.GetProductsOutOfStock({}, (err, res) => err ? reject(err) : resolve(res.products));
    }),

    // Factures
    facture: (_, { id }) => new Promise((resolve, reject) => {
      factureService.GetFacture({ id }, (err, res) => err ? reject(err) : resolve(res.facture));
    }),
    listFactures: () => new Promise((resolve, reject) => {
      factureService.ListFactures({}, (err, res) => err ? reject(err) : resolve(res.factures));
    }),
    getFacturesByStatus: (_, { status }) => new Promise((resolve, reject) => {
      factureService.GetFacturesByStatus({ status }, (err, res) => err ? reject(err) : resolve(res.factures));
    }),
    getFacturesByClient: (_, { clientId }) => new Promise((resolve, reject) => {
      factureService.GetFacturesByClient({ clientId }, (err, res) => err ? reject(err) : resolve(res.factures));
    }),
  },

  Mutation: {
    // Clients
    createClient: (_, args) => new Promise((resolve, reject) => {
      clientService.CreateClient({ client: args }, (err, res) => err ? reject(err) : resolve(res.client));
    }),
    updateClient: (_, args) => new Promise((resolve, reject) => {
      clientService.UpdateClient({ client: args }, (err, res) => err ? reject(err) : resolve(res.client));
    }),
    deleteClient: (_, { id }) => new Promise((resolve, reject) => {
      clientService.DeleteClient({ id }, (err, res) => err ? reject(err) : resolve(res.success));
    }),

    // Produits
    createProduct: (_, args) => new Promise((resolve, reject) => {
      productService.CreateProduct({ product: args }, (err, res) => err ? reject(err) : resolve(res.product));
    }),
    updateProduct: (_, args) => new Promise((resolve, reject) => {
      productService.UpdateProduct({ product: args }, (err, res) => err ? reject(err) : resolve(res.product));
    }),
    deleteProduct: (_, { id }) => new Promise((resolve, reject) => {
      productService.DeleteProduct({ id }, (err, res) => err ? reject(err) : resolve(res.success));
    }),

    // Factures
    createFacture: (_, args) => new Promise((resolve, reject) => {
      factureService.CreateFacture({ facture: args }, (err, res) => err ? reject(err) : resolve(res.facture));
    }),
    deleteFacture: (_, { id }) => new Promise((resolve, reject) => {
      factureService.DeleteFacture({ id }, (err, res) => err ? reject(err) : resolve(res.success));
    }),
  }
};

module.exports = resolvers;