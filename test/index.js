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
let asyncOpCount = 0;

reader.onEvent(op => {
  if (op.ns !== 'testdb.books') return;
  opCount += 1;
  console.log(op);
});

reader.onEvent(op => {
  if (op.ns !== 'testdb.books') return;
  return Promise.delay(500).then(() => {
    asyncOpCount += 1;
  });
});

const url = 'mongodb://localhost:27017/testdb';

MongoDB.MongoClient.connect(url).then(db => {
  return Promise.resolve()
    .then(() => db.collection('books').insert({ title: 'Hello 1', rand: Math.random() }))
    .then(() => console.log('inserted document 1'))
    .then(() => reader.start())
    .then(() => db.collection('books').insert({ title: 'Hello 2', rand: Math.random() }))
    .then(() => console.log('inserted document 2'))
    .delay(3000)
    .then(() => db.collection('books').insert({ title: 'Hello 3', rand: Math.random() }))
    .then(() => console.log('inserted document 3'))
    .delay(3000)
    .then(() => {
      assert.ok(opCount === 3, `Incorrect op count '${opCount}'`);
      assert.ok(asyncOpCount === 9, `Incorrect async op count '${asyncOpCount}'`);
    })
    .then(() => {
      console.log('Success.');
      process.exit(0);
    });
  })
  .catch(err => {
    console.log(err);
    process.exit(1);
  });

// TODO:
// setConnectionStrings
// reconnect
// disconnect a shard
