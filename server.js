require('dotenv').config();
const express = require('express');
const cors = require('cors')
const redisClient = require('./config/redis');
const zookeeperClient = require('./config/zookeeper');
const { initializeZNodes, generateUniqueId } = require('./services/zookeeperService');
const tokenBucketRateLimiter = require('./middlewares/tokenBucketRateLimiter');


const app = express();
app.use(cors());
const PORT = process.env.PORT || 8080;

async function initializeServices() {
  const maxRetries = 5;
  let attempt = 1;

  while (attempt <= maxRetries) {
    try {
      console.log(`🔄 Initialization attempt ${attempt}/${maxRetries}`);
      
      console.log('🔌 Connecting to Redis...');
      await redisClient.waitForConnection();
      
      console.log('🔗 Connecting to Zookeeper...');
      await zookeeperClient.connect();
      
      console.log('📂 Initializing ZNodes...');
      await initializeZNodes();

      console.log('connecting database');
      require('./config/db')();

      console.log('✅ All services initialized');
      return;
    } catch (err) {
      console.error(`⚠️ Initialization attempt ${attempt} failed:`, err.message);
      if (attempt === maxRetries) throw err;
      await new Promise(resolve => setTimeout(resolve, 10000));
      attempt++;
    }
  }
}

function setupRoutes() {
  app.use(express.json());
  // app.use(tokenBucketRateLimiter);
  app.get('/',(req,res) => res.send('heellp'))
  app.use('/api', require('./routes/urlRoutes'));
}

async function startServer() {
  try {
    await initializeServices();
    setupRoutes();
    
    app.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error('❌ Fatal startup error:', err);
    process.exit(1);
  }
}

process.on('SIGINT', async () => {
  console.log('\n🛑 Gracefully shutting down...');
  try {
    await redisClient.quit();
    await zookeeperClient.close();
    console.log('✅ All connections closed');
  } catch (err) {
    console.error('Shutdown error:', err);
  }
  process.exit();
});

startServer();
