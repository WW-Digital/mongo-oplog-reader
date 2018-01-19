const MongoOplogReader = require('..');
const redis = require('redis');

const reader = new MongoOplogReader({
  connectionStrings: [
    'mongodb://localhost:27017/local'
  ],
  redisClient: redis.createClient()
});

reader.on('op', op => console.log(op));

reader.tail();

// reader.setConnectionStrings(connectionStrings);