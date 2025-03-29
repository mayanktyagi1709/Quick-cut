// middlewares/tokenBucketRateLimiter.js
const redisClient = require('../config/redis');

const MAX_TOKENS = 10;
const REFILL_INTERVAL_SECONDS = 60 * 10;
const REFILL_RATE = 1 / REFILL_INTERVAL_SECONDS;

const tokenBucketRateLimiter = async (req, res, next) => {
  try {
    const userKey = req.ip;
    const tokensKey = `bucket:tokens:${userKey}`;
    const timestampKey = `bucket:lastRefill:${userKey}`;
    const now = Math.floor(Date.now() / 1000);

    // Get tokens and last refill
    let tokens = await redisClient.get(tokensKey);
    let lastRefill = await redisClient.get(timestampKey);

    console.log('🔍 Redis fetched:', { tokens, lastRefill });

    tokens = parseFloat(tokens);
    if (isNaN(tokens)) tokens = MAX_TOKENS;

    lastRefill = parseInt(lastRefill);
    if (isNaN(lastRefill)) lastRefill = now;

    const elapsed = now - lastRefill;
    tokens = Math.min(MAX_TOKENS, tokens + elapsed * REFILL_RATE);

    if (tokens < 1) {
      const nextRefillIn = Math.ceil((1 - tokens) / REFILL_RATE);
      return res.status(429).json({
        success: false,
        message: `Rate limit exceeded. Try again after ${nextRefillIn} seconds.`,
      });
    }

    tokens -= 1;

    console.log(`✅ Tokens left: ${tokens.toFixed(2)} after consuming one`);

    // Save tokens and last refill
    await redisClient.set(tokensKey, tokens.toString());
    await redisClient.set(timestampKey, now.toString());

    next();
  } catch (err) {
    console.error('❌ Rate limiter error:', err);
    return res.status(500).json({ message: 'Internal Server Error' });
  }
};

module.exports = tokenBucketRateLimiter;

