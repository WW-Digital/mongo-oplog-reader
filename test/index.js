const Promise = require('bluebird');
const MongoDB = require('mongodb');
const MongoOplogReader = require('..');
const redis = require('redis');
const assert = require('assert');

const reader = new MongoOplogReader({
  connectionStrings: [
    'mongodb://localhost:27018/local',
    'mongodb://localhost:27028/local',
    'mongodb://localhost:27038/local',
    'mongodb://localhost:27118/local',
    'mongodb://localhost:27128/local',
    'mongodb://localhost:27138/local'
  ],
  redisClient: redis.createClient()
});

let opCount = 0;
let shardOpCount = 0;

reader.on('op', op => {
  opCount += 1;
  console.log(op);
});

reader.on('shard-op', op => {
  shardOpCount += 1;
});

reader.start().catch(console.log);

const url = 'mongodb://localhost:27017/testdb';

Promise.resolve()
  .then(() => MongoDB.MongoClient.connect(url))
  .then(db => db.collection('books').insert({ title: 'Hello', rand: Math.random() }))
  .delay(1000)
  .then(() => {
    assert.ok(opCount === 1, `Incorrect op count '${opCount}'`);
    assert.ok(shardOpCount === 3, `Incorrect shard op count '${shardOpCount}'`);
  })
  .then(() => {
    console.log('Success.');
    process.exit(0);
  })
  .catch(err => {
    console.log(err);
    process.exit(1);
  });

// TODO:
// setConnectionStrings
// reconnect
// disconnect a shard
