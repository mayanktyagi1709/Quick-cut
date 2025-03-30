// utils/base62.js

const chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const base = chars.length;

function encode(num) {
  let encoded = '';
  while (num > 0) {
    encoded = chars[num % base] + encoded;
    num = Math.floor(num / base);
  }

  // Pad to 7 characters (optional but required for fixed-length codes)
  while (encoded.length < 7) {
    encoded = '0' + encoded;
  }

  return encoded;
}

module.exports = {
  encode,
};
