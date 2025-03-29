FROM node:18-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 8080

# Start the app
CMD ["node", "server.js"]


