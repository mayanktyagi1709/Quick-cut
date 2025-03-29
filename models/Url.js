const mongoose = require('mongoose');

const urlSchema = mongoose.Schema({
  shortCode : {
    type: String,
    required: true,
    unique: true,
    index: true,
    length: 7
  },
  longUrl: {
    type: String,
    required: true
  },
  createdAt: {
    type: Date,
    default: Date.now
  },
  clicks: {
    type: Number,
    default: 0
  }
})

module.exports = mongoose.model('Url',urlSchema);