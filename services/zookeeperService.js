const zookeeper = require('node-zookeeper-client');
const zookeeperClient = require('../config/zookeeper'); 

const COUNTER_PATHS = ['/counter/node1', '/counter/node2'];
const MAX_SEQUENCE = 999999;

async function ensureZNodeExists(client, path, initialData = '0') {
  return new Promise((resolve, reject) => {
    client.exists(path, (err, stat) => {
      if (err) return reject(err);
      if (stat) return resolve();
      
      client.create(
        path,
        Buffer.from(initialData),
        zookeeper.CreateMode.PERSISTENT,
        (err) => {
          if (err?.code === -110) return resolve();
          if (err) return reject(err);
          resolve();
        }
      );
    });
  });
}

async function initializeZNodes() {
  const clients = zookeeperClient.getConnectedClients();
  if (clients.length === 0) throw new Error('No Zookeeper clients connected');

  for (const [index, client] of clients.entries()) {
    await ensureZNodeExists(client, '/counter');
    await ensureZNodeExists(client, COUNTER_PATHS[index], '0');
  }
}

function getAndIncrement(client, path, offset) {
  return new Promise((resolve, reject) => {
    client.getData(path, (err, data, stat) => {
      if (err) return reject(err);
      
      const current = parseInt(data.toString()) || 0;
      if (current > MAX_SEQUENCE) {
        return reject(new Error('Sequence overflow'));
      }

      client.setData(
        path,
        Buffer.from(String(current + 1)),
        stat.version,
        (err) => {
          if (err) return reject(err);
          resolve(offset + current + 1);
        }
      );
    });
  });
}

async function generateUniqueId() {
  const clients = zookeeperClient.getConnectedClients();
  const attempts = new Set();
  
  while (attempts.size < clients.length) {
    const index = Math.floor(Math.random() * clients.length);
    if (attempts.has(index)) continue;
    
    try {
      return await getAndIncrement(
        clients[index],
        COUNTER_PATHS[index],
        (index + 1) * 1000000
      );
    } catch (err) {
      attempts.add(index);
    }
  }
  throw new Error('All Zookeeper nodes failed');
}

module.exports = { initializeZNodes, generateUniqueId };