# mongo-oplog-reader

Read MongoDB oplogs from a sharded cluster.

- Supports multiple processes (backed by Redis) for High Availability
- Emits event after document has been written to replica set majority
- Resumes from the last emitted event (in the case of process crash/stop/restart)
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

const reader = new MongoOplogReader({ connectionStrings, redisClient });
reader.on('op', op => console.log(op));
reader.tail();
reader.setConnectionStrings(connectionStrings);
```

## License

MIT