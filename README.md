# mongo-oplog-reader

Read MongoDB oplogs from a sharded cluster.

[![Build Status](https://travis-ci.org/WW-Digital/mongo-oplog-reader.svg?branch=master)](https://travis-ci.org/WW-Digital/mongo-oplog-reader)

- Supports multiple processes for high availability
- Emits event after document has been written to the majority of members of the shard's replica set
- Resumes from the last emitted event of the replica set (in the case of process crash/stop/restart)
- Ability to add/remove shards/hosts without restarting

## Install

```
npm i mongo-oplog-reader redis
```

## Usage

```js
import MongoOplogReader from 'mongo-oplog-reader';
import redis from 'redis';

const redisClient = redis.createClient();

const connectionStrings = [
  'mongodb://shard0-primary/local',
  'mongodb://shard0-secondary0/local',
  'mongodb://shard0-secondary1/local',
  'mongodb://shard1-primary/local',
  'mongodb://shard1-secondary0/local',
  'mongodb://shard1-secondary1/local'
];

const reader = new MongoOplogReader({ redisClient });
reader.setConnectionStrings(connectionStrings);
reader.on('op', op => console.log(op));
reader.start();
```

## License

MIT
