\# Projet SOA \& Microservices

> Architecture microservices avec gRPC, REST, GraphQL et Kafka

> Module : SoA et Microservices | A.U. 2025-26

\---

\## equipe

| Membre | Role |

|--------|------|

| Salma Chalbi | Microservices Clients + Produits + Factures  |

| Fethi ben hlima | API Gateway + Integration Kafka  |

\---

\## Schema d Architecture



```

+-----------------------------------------------------------------+

|                       POSTMAN / CLIENT                          |

+-------------------------+---------------------------------------+

&#x20;                         | HTTP REST / GraphQL

&#x20;                         v

+-----------------------------------------------------------------+

|                   API GATEWAY (Port 3000)                       |

|            REST (Express) + GraphQL (Apollo)                    |

+----------+-------------+------------------+--------------------+

&#x20;          | gRPC        | gRPC             | gRPC

&#x20;          v             v                  v

+------------+  +--------------+  +------------------+

| Microserv. |  |  Microserv.  |  |   Microserv.     |

|  Clients   |  |   Produits   |  |    Factures      |

| Port 50051 |  |  Port 50052  |  |   Port 50053     |

|  SQLite3   |  |    RxDB      |  |    SQLite3       |

+------------+  +------+-------+  +--------+---------+

&#x20;                      |                   |

&#x20;                      |   +--------------+

&#x20;                      |   | Kafka Event: FACTURE\_CREATED

&#x20;                      v   v

&#x20;                +-----------+

&#x20;                |   KAFKA   |

&#x20;                |  Broker   |

&#x20;                | Port 9092 |

&#x20;                +-----------+



Topics Kafka :

&#x20; clients-events   <- CLIENT\_CREATED / CLIENT\_UPDATED / CLIENT\_DELETED

&#x20; products-events  <- PRODUCT\_CREATED / PRODUCT\_UPDATED / PRODUCT\_DELETED

&#x20; factures-events  <- FACTURE\_CREATED / FACTURE\_DELETED

&#x20;                     -> Consomme par Produits -> decremente le stock

```



\---



\## Structure du Projet



```

projet-soa/

├── microservice-clients/         

│   ├── server.js

│   ├── db.js

│   ├── client.proto

│   └── client.js

├── microservice-produits/         

│   ├── server.js

│   ├── db.js

│   ├── product.proto

│   └── product.js

├── microservice-factures/         # Salma

│   ├── server.js

│   ├── db.js

│   ├── facture.proto

│   └── facture.js

├── api-gateway/                   

│   ├── apiGateway.js

│   ├── resolvers.js

│   └── schema.gql

├── kafka/                         

│   ├── kafkaProducer.js

│   ├── kafkaConsumer.js

│   └── kafkaService.js

├── postman\_collection.json

├── docker-compose.yml

└── README.md

```



\---

\## Prerequis



\- \*\*Node.js\*\* v18+ -> https://nodejs.org

\- \*\*Docker Desktop\*\* -> https://www.docker.com/products/docker-desktop

\- \*\*Postman\*\* -> https://www.postman.com

\---

\## Installation et Execution

```bash

git clone https://github.com/salmachalbi/projet-soa.git

cd projet-soa



\# Installer les dependances

cd microservice-clients \&\& npm install \&\& cd ..

cd microservice-produits \&\& npm install \&\& cd ..

cd microservice-factures \&\& npm install \&\& cd ..

cd api-gateway \&\& npm install \&\& cd ..

cd kafka \&\& npm install \&\& cd ..



\# Lancer Kafka

docker-compose up -d



\# Lancer les services (4 terminaux)

cd microservice-clients \&\& node server.js

cd microservice-produits \&\& node server.js

cd microservice-factures \&\& node server.js

cd api-gateway \&\& node apiGateway.js

```

\## Endpoints REST

Base URL : http://localhost:3000

\### Clients

| Methode | Endpoint | Description |

|---------|----------|-------------|

| GET | /clients | Lister tous les clients |

| GET | /clients/:id | Obtenir un client |

| POST | /clients | Creer un client |

| PUT | /clients/:id | Modifier un client |

| DELETE | /clients/:id | Supprimer un client |

| GET | /clients/search?name=X | Rechercher par nom |



\### Produits

| Methode | Endpoint | Description |

|---------|----------|-------------|

| GET | /products | Lister tous les produits |

| GET | /products/:id | Obtenir un produit |

| POST | /products | Creer un produit |

| PUT | /products/:id | Modifier un produit |

| DELETE | /products/:id | Supprimer un produit |

| GET | /products/search?name=X | Rechercher par nom |

| GET | /products/category?category=X | Filtrer par categorie |

| GET | /products/outofstock | Produits hors stock |


\### Factures

| Methode | Endpoint | Description |

|---------|----------|-------------|

| GET | /factures | Lister toutes les factures |

| GET | /factures/:id | Obtenir une facture |

| POST | /factures | Creer une facture |

| DELETE | /factures/:id | Supprimer une facture |

| GET | /factures/status?status=X | Filtrer par statut |

| GET | /factures/client?clientId=X | Factures d'un client |

\---

\## GraphQL — POST http://localhost:3000/graphql

\*\*Queries :\*\* client, listClients, searchClientByName, product, listProducts, searchProductByName, getProductsByCategory, getProductsOutOfStock, facture, listFactures, getFacturesByStatus, getFacturesByClient

\*\*Mutations :\*\* createClient, updateClient, deleteClient, createProduct, updateProduct, deleteProduct, createFacture, deleteFacture

\## gRPC (.proto)



| Fichier | Port | RPCs principaux |

|---------|------|-----------------|

| client.proto | 50051 | CreateClient, GetClient, UpdateClient, DeleteClient, ListClients, SearchClientByName |

| product.proto | 50052 | CreateProduct, GetProduct, UpdateProduct, DeleteProduct, ListProducts, SearchProductByName, GetProductsByCategory, GetProductsOutOfStock |

| facture.proto | 50053 | CreateFacture, GetFacture, DeleteFacture, ListFactures, GetFacturesByStatus, GetFacturesByClient |

\## Topics Kafka

| Topic | Producteur | Consommateur | Events |

|-------|-----------|--------------|--------|

| clients-events | Microservice Clients | - | CLIENT\_CREATED, CLIENT\_UPDATED, CLIENT\_DELETED |

| products-events | Microservice Produits | - | PRODUCT\_CREATED, PRODUCT\_UPDATED, PRODUCT\_DELETED |

| factures-events | Microservice Factures | Microservice Produits | FACTURE\_CREATED, FACTURE\_DELETED |


\## Bases de Donnees

| Microservice | Type | Technologie | Justification |

|-------------|------|-------------|---------------|

| Clients | SQL | SQLite3 | Donnees structurees, relations simples |

| Produits | NoSQL | RxDB + SQLite storage | Schema flexible, champ photo, requetes reactives |

| Factures | SQL | SQLite3 | Donnees financieres structurees, integrite |


\## Ports utilises

| Service | Port |

|---------|------|

| API Gateway (REST + GraphQL) | 3000 |

| Microservice Clients (gRPC) | 50051 |

| Microservice Produits (gRPC) | 50052 |

| Microservice Factures (gRPC) | 50053 |

| Kafka Broker | 9092 |

| Zookeeper | 2181 |


\## Technologies utilisees


\- \*\*Node.js\*\* - Runtime JavaScript

\- \*\*gRPC + Protocol Buffers\*\* - Communication inter-microservices

\- \*\*Express.js\*\* - API REST

\- \*\*Apollo Server\*\* - Serveur GraphQL

\- \*\*KafkaJS\*\* - Client Kafka pour Node.js

\- \*\*SQLite3\*\* - Base de donnees SQL

\- \*\*RxDB\*\* - Base de donnees NoSQL reactive

\- \*\*Docker\*\* - Conteneurisation Kafka/Zookeeper

