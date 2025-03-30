const zookeeper = require('node-zookeeper-client');
const zookeeperClient = require('../config/zookeeper');
const { COUNTER_PATHS, BASE_COUNTER_PATH, MAX_SEQUENCE } = require('../config/constants');

async function ensureZNodeExists(client, path, initialData = '0') {
  const stat = await new Promise((resolve, reject) =>
    client.exists(path, (err, stat) => (err ? reject(err) : resolve(stat)))
  );

  if (!stat) {
    try {
      await new Promise((resolve, reject) =>
        client.create(
          path,
          Buffer.from(initialData),
          zookeeper.CreateMode.PERSISTENT,
          (err) => (err?.code === -110 ? resolve() : err ? reject(err) : resolve())
        )
      );
    } catch (err) {
      console.error(`Error creating ZNode at ${path}:`, err);
      throw err;
    }
  }
}

async function initializeZNodes() {
  const clients = zookeeperClient.getConnectedClients();
  if (clients.length === 0) throw new Error('No Zookeeper clients connected');

  await Promise.all(
    clients.map((client, index) =>
      Promise.all([
        ensureZNodeExists(client, BASE_COUNTER_PATH),
        ensureZNodeExists(client, COUNTER_PATHS[index], '0'),
      ])
    )
  );
}

function getAndIncrement(client, path, offset) {
  return new Promise((resolve, reject) => {
    client.getData(path, (err, data, stat) => {
      if (err) {
        console.error(`Error getting data for ZNode at ${path}:`, err);
        return reject(err);
      }

      const current = parseInt(data.toString()) || 0;
      if (current > MAX_SEQUENCE) {
        return reject(new Error('Sequence overflow'));
      }

      client.setData(
        path,
        Buffer.from(String(current + 1)),
        stat.version,
        (err) => {
          if (err) {
            console.error(`Error setting data for ZNode at ${path}:`, err);
            return reject(err);
          }
          resolve(offset + current + 1);
        }
      );
    });
  });
}

async function generateUniqueId() {
  const clients = zookeeperClient.getConnectedClients();

  for (let i = 0; i < clients.length; i++) {
    const index = Math.floor(Math.random() * clients.length);

    try {
      return await getAndIncrement(
        clients[index],
        COUNTER_PATHS[index],
        (index + 1) * 1000000
      );
    } catch (err) {
      console.error(`Failed to increment counter for client ${index}:`, err);
    }
  }

  throw new Error('All Zookeeper nodes failed');
}

module.exports = { initializeZNodes, generateUniqueId };