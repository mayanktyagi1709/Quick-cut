// services/redisService.js
const redisClient = require('../config/redis');

const CACHE_EXPIRY_SECONDS = 3600; // 1 hour (adjust as needed)

/**
 * Get a value from Redis cache by key.
 * If not found, returns null.
 */
const getFromCache = async (key) => {
  try {
    const data = await redisClient.get(key);
    return data ? JSON.parse(data) : null;
  } catch (err) {
    console.error('❌ Redis Get Error:', err);
    return null;
  }
};

/**
 * Set a value into Redis cache.
 * Automatically expires after CACHE_EXPIRY_SECONDS.
 */
const setToCache = async (key, value) => {
  try {
    await redisClient.setEx(key, CACHE_EXPIRY_SECONDS, JSON.stringify(value));
  } catch (err) {
    console.error('❌ Redis Set Error:', err);
  }
};

/**
 * Delete a value from cache by key.
 */
const deleteFromCache = async (key) => {
  try {
    await redisClient.del(key);
  } catch (err) {
    console.error('❌ Redis Delete Error:', err);
  }
};

module.exports = {
  getFromCache,
  setToCache,
  deleteFromCache,
};
