// routes/url.routes.js
const express = require('express');
const router = express.Router();

const tokenBucketRateLimiter = require('../middlewares/tokenBucketRateLimiter');
const { shortenUrl, redirectToLongUrl } = require('../controllers/urlController');

router.post('/create', tokenBucketRateLimiter, shortenUrl);
router.get('/:shortCode', redirectToLongUrl);

module.exports = router;
