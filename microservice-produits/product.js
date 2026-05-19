const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = './product.proto';
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const productProto = grpc.loadPackageDefinition(packageDef).product;

const client = new productProto.ProductService(
  'localhost:50052',
  grpc.credentials.createInsecure()
);

// Tester les méthodes
client.CreateProduct({ product: { id: "p1", name: "Laptop", price: 1200, stock: 10 , category: "Électronique", photo: "laptop.jpg" } }, (err, res) => {
  if (err) return console.error("Erreur Create:", err.message);
  console.log("Create:", res.product);

  client.GetProduct({ id: "p1" }, (err, res) => {
    if (err) return console.error("Erreur Get:", err.message);
    console.log("Get:", res.product);

    client.UpdateProduct({ product: { id: "p1", name: "Laptop Pro", price: 1500, stock: 8 , category: "Électronique", photo: "laptop_pro.jpg" } }, (err, res) => {
      if (err) return console.error("Erreur Update:", err.message);
      console.log("Update:", res.product);

      client.ListProducts({}, (err, res) => {
        if (err) return console.error("Erreur List:", err.message);
        console.log("List:", res.products);

        client.DeleteProduct({ id: "p1" }, (err, res) => {
          if (err) return console.error("Erreur Delete:", err.message);
          console.log("Delete:", res.success);

client.SearchProductByName({ name: "Laptop" }, (err, res) => {
  if (err) return console.error("Erreur Search:", err.message);
  console.log("Search:", res.products);

  client.GetProductsByCategory({ category: "Électronique" }, (err, res) => {
    if (err) return console.error("Erreur GetByCategory:", err.message);
    console.log("GetByCategory:", res.products);


client.GetProductsOutOfStock({}, (err, res) => {
  if (err) return console.error("Erreur OutOfStock:", err.message);
  console.log("Produits hors stock:", res.products);




});


  });


        });
      });
    });
  });
});});
