const zookeeper = require('node-zookeeper-client');

class ZookeeperClient {
  constructor() {
    this.clients = []; // Properly initialized
  }

  async connect() {
    const urls = process.env.ZOOKEEPER_URLS?.split(',') || [];
    
    for (const [index, url] of urls.entries()) {
      const client = zookeeper.createClient(url.trim(), {
        sessionTimeout: 30000,
        retries: 10,
        spinDelay: 1000
      });
      
      await new Promise((resolve, reject) => {
        client.once('connected', () => {
          console.log(`✅ Zookeeper client ${index + 1} connected to ${url}`);
          this.clients.push(client);
          resolve();
        });
        
        client.once('error', reject);
        client.connect();
      });
    }
  }

  getConnectedClients() {
    return this.clients.filter(client => 
      client.getState() === zookeeper.State.SYNC_CONNECTED
    );
  }

  async close() {
    for (const client of this.clients) {
      client.close();
    }
    console.log('🔌 All Zookeeper connections closed');
  }
}

// Singleton instance
module.exports = new ZookeeperClient();