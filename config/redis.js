const { createClient } = require('redis');

class RedisClient {
  constructor() {
    this.client = createClient({
      url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
      socket: {
        connectTimeout: 30000,
        reconnectStrategy: (retries) => {
          console.log(`♻️ Redis reconnecting (attempt ${retries + 1})`);
          return Math.min(retries * 500, 5000);
        }
      }
    });

    this.connected = false;
    
    this.client
      .on('connect', () => console.log('🔌 Redis connection established'))
      .on('ready', () => {
        this.connected = true;
        console.log('✅ Redis ready');
      })
      .on('error', (err) => console.error('❌ Redis error:', err))
      .on('end', () => {
        this.connected = false;
        console.log('🔴 Redis connection closed');
      });
  }

  async waitForConnection() {
    if (this.connected) return;
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Redis connection timeout'));
      }, 30000);

      this.client.connect()
        .then(() => {
          clearTimeout(timeout);
          resolve();
        })
        .catch(reject);
    });
  }

  async get(key) {
    await this.waitForConnection();
    return this.client.get(key);
  }

  async set(key, value) {
    await this.waitForConnection();
    return this.client.set(key, value);
  }

  async quit() {
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }
}

module.exports = new RedisClient();