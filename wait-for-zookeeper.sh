#!/bin/sh

echo "⏳ Waiting for Zookeeper 1 on zookeeper1:2181..."
while ! nc -z zookeeper1 2181; do
  sleep 2
done
echo "✅ Zookeeper 1 is up"

echo "⏳ Waiting for Zookeeper 2 on zookeeper2:2182..."
while ! nc -z zookeeper2 2182; do
  sleep 2
done
echo "✅ Zookeeper 2 is up"

# Finally start the node app
echo "🚀 Starting Node.js backend..."
npm run dev
