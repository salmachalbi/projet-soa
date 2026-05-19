'use strict';
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, 'client.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const { client: clientProto } = grpc.loadPackageDefinition(packageDef);

const client = new clientProto.ClientService(
  'localhost:50051',
  grpc.credentials.createInsecure()
);

// Test CreateClient
client.CreateClient(
  { client: { name: 'Ali Ben Salah', email: 'ali@example.com', phone: '+21698765432' } },
  (err, response) => {
    if (err) {
      console.error('CreateClient - Erreur :', err.message);
      return;
    }
    console.log('CreateClient - Réponse :', response.client);
    const clientId = response.client.id;

    // Test GetClient
    client.GetClient({ id: clientId }, (err, response) => {
      if (err) {
        console.error('GetClient - Erreur :', err.message);
      } else {
        console.log('GetClient - Réponse :', response.client);
      }

      // Test SearchClientByName
      client.SearchClientByName({ name: 'Ali' }, (err, response) => {
        if (err) {
          console.error('SearchClientByName - Erreur :', err.message);
        } else {
          console.log('SearchClientByName - Réponse :', response.clients);
        }

        // Test ListClients
        client.ListClients({ page_size: 10, page_token: '' }, (err, response) => {
          if (err) {
            console.error('ListClients - Erreur :', err.message);
          } else {
            console.log('ListClients - Réponse :', response.clients);
          }

          // Test UpdateClient
          client.UpdateClient(
            { client: { id: clientId, name: 'Ali Updated', email: 'ali.updated@example.com', phone: '+21698765433' } },
            (err, response) => {
              if (err) {
                console.error('UpdateClient - Erreur :', err.message);
              } else {
                console.log('UpdateClient - Réponse :', response.client);
              }

              // Test DeleteClient
              client.DeleteClient({ id: clientId }, (err, response) => {
                if (err) {
                  console.error('DeleteClient - Erreur :', err.message);
                } else {
                  console.log('DeleteClient - Réponse :', response.success);
                }
              });
            }
          );
        });
      });
    });
  }
);
