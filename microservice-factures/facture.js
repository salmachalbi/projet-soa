const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = './facture.proto';
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const factureProto = grpc.loadPackageDefinition(packageDef).facture;

const client = new factureProto.FactureService(
  'localhost:50053',
  grpc.credentials.createInsecure()
);

client.CreateFacture({
  facture: {
    id: "f1",
    clientId: "c1",
    productId: "p1",
    quantity: 2,
    total: 2400,
    date: "2026-05-14",
    status: "PAYÉ"
  }
}, (err, res) => {
  if (err) return console.error("Erreur Create:", err.message);
  console.log("Create:", res.facture);

  client.GetFacture({ id: "f1" }, (err, res) => {
    if (err) return console.error("Erreur Get:", err.message);
    console.log("Get:", res.facture);

    client.ListFactures({}, (err, res) => {
      if (err) return console.error("Erreur List:", err.message);
      console.log("List:", res.factures);

      client.GetFacturesByClient({ clientId: "c1" }, (err, res) => {
        if (err) return console.error("Erreur GetByClient:", err.message);
        console.log("GetByClient:", res.factures);

        client.GetFacturesByStatus({ status: "PAYÉ" }, (err, res) => {
          if (err) return console.error("Erreur GetByStatus:", err.message);
          console.log("GetByStatus:", res.factures);

          client.DeleteFacture({ id: "f1" }, (err, res) => {
            if (err) return console.error("Erreur Delete:", err.message);
            console.log("Delete:", res.success);
          });
        });
      });
    });
  });
});
