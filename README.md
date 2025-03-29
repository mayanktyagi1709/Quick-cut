
# 🚀 Scalable URL Shortener System

A high-performance, scalable, and highly available URL shortener service built using **Node.js, Express, MongoDB, Redis, and Zookeeper**.

## 📌 Tech Stack

- **Node.js + Express** – API Server  
- **MongoDB Atlas** – Persistent storage for original and shortened URLs  
- **Redis** – Caching layer with LRU eviction  
- **Zookeeper** – Unique short code generation using sequential IDs  
- **Base62 Encoding** – Generates compact 7-character short codes  
- **Rate Limiting** – Token Bucket Algorithm via Redis

## 🔧 Features

- ✨ Unique 7-character short URL generation  
- ⚡ Fast redirection using Redis cache  
- 📦 Write-through and read-through caching strategy  
- 🔐 Rate limiting per user/IP (5 links/hour)  
- 📈 Click tracking  
- 🧠 Zookeeper-based distributed ID generation  
- 🗃 MongoDB with index on `shortCode` for fast lookups  
- 🧹 LRU cache eviction in Redis when full  
- 🛡 Spam protection and URL validation  

## ⚙️ Architecture Overview

```
Client ↔ Backend API (Express)
           ↓
   Redis Cache (Rate Limit, Token Bucket)
           ↓
   Zookeeper Nodes (ID Generation)
           ↓
   MongoDB Atlas (URL storage)
```

---

## 📂 Project Structure

```
url-shortener/
├── controllers/
│   └── urlsController.js
├── services/
│   ├── redisService.js
│   ├── zookeeperService.js
│   └── rateLimiter.js
├── models/
│   └── Url.js
├── routes/
│   └── urlRoutes.js
├── utils/
│   └── base62.js
├── config/
│   ├── db.js
│   ├── redis.js
│   └── zookeeper.js
├── app.js
├── server.js
├── .env
└── docker-compose.yml
```

## 📜 API Endpoints

### `POST /api/shorten`
- Request Body:
  ```json
  {
    "originalUrl": "https://example.com"
  }
  ```
- Response:
  ```json
  {
    "shortUrl": "https://yourdomain.com/abc123X"
  }
  ```

### `GET /:shortCode`
- Redirects to the original long URL.

## ⚙️ Environment Variables (`.env`)

```
PORT=5000
MONGODB_URI=your_mongodb_atlas_connection_string
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
BASE_URL=http://localhost:5000
RATE_LIMIT=5
RATE_LIMIT_WINDOW=3600
```

## 🐳 Running with Docker

### Start Redis and Zookeeper via Docker:
```bash
docker-compose up -d
```

### Example `docker-compose.yml`:
```yaml
services:
  redis:
    image: redis:latest
    container_name: redis-server
    ports:
      - "6379:6379"

  zookeeper:
    image: zookeeper:latest
    container_name: zookeeper-server
    ports:
      - "2181:2181"
```

## 🏁 Running the Project Locally

### 1. Clone the repo
```bash
git clone https://github.com/your-username/url-shortener.git
cd url-shortener
```

### 2. Install dependencies
```bash
npm install
```

### 3. Start the server
```bash
npm run dev
```

> Ensure Docker containers for Redis & Zookeeper are running before starting the server.

## 🧠 How Unique ID Generation Works

- Three Zookeeper nodes assigned offset ranges:
  - Node1: 1000000 - 1999999
  - Node2: 2000000 - 2999999
  - Node3: 3000000 - 3999999 (to be added)
- Random client chosen for ID generation.
- If one node's sequence exceeds max range, fallback to next node.
- All IDs are encoded using Base62 for shortness.

---

## 🛡 Rate Limiting

- Token Bucket Algorithm using Redis.
- Config:
  - Max Tokens: 10
  - Refill Rate: 1 token every 10 minutes
- Based on user IP to prevent abuse.

---

## 🧪 Health Checks

- Redis: Custom `incr` command test
- Zookeeper: `zkServer.sh status` healthcheck

---

## 📄 Future Improvements

- Frontend UI (React )
- Analytics Dashboard
- Custom aliases
- User authentication & history

---

## 📈 Scalability & Performance

- Horizontal scaling with Load Balancer  
- MongoDB sharding support  
- Redis replication/failover  
- Stateless API servers  
- Rate limiting to prevent abuse  
- LRU-based cache eviction  

## 📚 System Design Highlights

- Zookeeper ensures **globally unique IDs** even in a distributed setup  
- Redis caching improves **read performance** drastically  
- Token Bucket Rate Limiting ensures **fair usage**  
- All short codes are **7-character Base62 encoded** for URL friendliness  

## 🔐 Security Considerations

- URL validation using regex  
- IP-based rate limiting  
- Blacklisting malicious domains  

## ✅ Future Enhancements

- User authentication and analytics dashboard  
- Expiry handling and scheduled cleanup  
- QR code generation  
- Custom short code support  

## 🧠 Learn More

- Zookeeper Coordination & Sequential Nodes  
- Redis Caching Patterns  
- Token Bucket vs Sliding Window  
- Base62 Encoding Algorithm  
- LRU Cache Strategy  

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).

## 👨‍💻 Author

**Rohit Kumar**  
📧 [rohit.kumpan01@gmail.com](mailto:rohit.kumpan01@gmail.com)  
🌐 [GitHub: @rohit9123](https://github.com/rohit9123)
