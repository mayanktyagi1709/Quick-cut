// controllers/urlController.js

const Url = require('../models/Url');
const { encode } = require('../utils/base62');
const { generateUniqueId } = require('../services/zookeeperService');
const redisClient = require('../config/redis'); // We'll integrate Redis later
const { getFromCache, setToCache } = require('../services/redisService');

// POST /api/url/shorten
const shortenUrl = async (req, res) => {
  const { longUrl } = req.body;
  console.log(longUrl)
  if (!longUrl) return res.status(400).json({ message: 'Long URL is required' });

  try {
    // Step 1: Generate Unique ID from Zookeeper
    const uniqueId = await generateUniqueId();

    // Step 2: Encode to Base62 7-char string
    const shortCode = encode(uniqueId);

    // Step 3: Save in MongoDB
    const newUrl = new Url({ longUrl, shortCode });
    await newUrl.save();

    // Optional: Save in Redis cache
    // await redisClient.set(shortCode, longUrl);

    return res.status(201).json({
      shortCode,
      shortUrl: `${process.env.BASE_URL}/${shortCode}`,
      longUrl,
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: 'Server Error' });
  }
};

// GET /api/url/:shortCode
const redirectToLongUrl = async (req, res) => {
  const { shortCode } = req.params;

  try {
    // Step 1: Check Redis Cache
   
    const cachedUrl = await getFromCache(shortCode);
    if (cachedUrl) {
      return res.redirect(cachedUrl.originalUrl);
    }

    // Step 2: Check MongoDB if not in cache

    const url = await Url.findOne({ shortCode });
    if (url) {
      await setToCache(shortCode, url);
      return res.redirect(url.originalUrl);
    }


    if (!url) {
      return res.status(404).json({ message: 'Short URL not found' });
    }

  } catch (error) {
    console.error('❌ Error in redirectShortUrl:', error);
    return res.status(500).json({ message: 'Server error' });
  }
};


module.exports = {
  shortenUrl,
  redirectToLongUrl,
};
