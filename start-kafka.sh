#!/bin/bash

# Kafka Setup Script

echo "🚀 Starting Kafka and Zookeeper..."
docker-compose up -d

echo "⏳ Waiting for Kafka to be ready..."
sleep 10

echo "✅ Kafka is ready!"
echo ""
echo "📌 Kafka Connection Details:"
echo "   Bootstrap Server: localhost:9092"
echo "   Zookeeper: localhost:2181"
echo ""
echo "📚 Topics to be auto-created:"
echo "   - clients-events"
echo "   - factures-events"
echo "   - products-events"
echo ""
echo "📦 Install dependencies in each microservice:"
echo "   cd microservice-clients && npm install"
echo "   cd ../microservice-factures && npm install"
echo "   cd ../microservice-produits && npm install"
echo ""
echo "🛑 To stop Kafka:"
echo "   docker-compose down"
