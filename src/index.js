/**
 * Reliable Mongo sharded cluster oplog tailing
 *
 * - Will emit events only after majority write
 * - Will emit events only once even if multiple processes are running
 * - Will resume tailing after the last emitted op
 * - Ability to accept a new set of oplog hosts (i.e. if a shard is added)
 *
 * More info:
 *  - https://www.mongodb.com/blog/post/tailing-mongodb-oplog-sharded-clusters
 *  - https://www.mongodb.com/blog/post/pitfalls-and-workarounds-for-tailing-the-oplog-on-a-mongodb-sharded-cluster
 */

const MongoDB = require('mongodb');
const MongoOplog = require('mongo-oplog');
const EventEmitter = require('eventemitter3');

const Timestamp = MongoDB.Timestamp;
const defaultKeyPrefix = 'mongoOplogReader';
const defaultTTL = 60 * 60 * 24;

class MongoOplogReader extends EventEmitter {

  /**
   * Instantiate a new MongoOplogReader
   * @param options
   *  - connectionStrings {String[]} a list of mongodb connection strings
   *  - redisClient {Object}
   *  - redundancy {Integer} number of processes per shard
   *  - ttl {Integer} max time it may take a cluster to recover (in seconds)
   *  - keyPrefix {String} the redis key prefix (default: 'mongoOplogReader')
   */
  constructor(options) {
    super();
    if (!Array.isArray(options.connectionStrings) || !options.connectionStrings.length) {
      throw new Error('connectionStrings is required.');
    }
    if (!options.redisClient || !options.redisClient.sadd) {
      throw new Error('redisClient must support: expire, get, sadd, scard, set, setnx');
    }
    this.connectionStrings = options.connectionStrings;
    this.redisClient = options.redisClient;
    this.oplogs = {};
    this.replSets = {};
    this.keyPrefix = options.keyPrefix || defaultKeyPrefix;
    this.ttl = options.ttl || defaultTTL;
  }

  /**
   * Begin tailing all oplogs
   * @param options
   *  - since {BsonTimestamp|Int} - the timestamp to start tailing the oplogs from
   */
  tail(options) {
    if (options && options.since) {
      return this.connectionStrings.forEach(connStr => this.tailHost(connStr, options));
    }
    this.getLastOpTimestamp()
      .then(ts => {
        const opts = Object.assign({}, options, { since: ts });
        this.connectionStrings.forEach(connStr => this.tailHost(connStr, opts));
      })
      .catch(console.log);
  }

  getLastOpTimeKey(replSetName) {
    return `${this.keyPrefix}:lastOpTime:${replSetName}`;
  }

  setLastOpTimestamp(replSetName, data) {
    const key = this.getLastOpTimeKey(replSetName);
    const unixTime = data.ts.high_;
    const opSeq = data.ts.low_;
    const val = `${unixTime}|${opSeq}`;
    this.redisClient.set(key, val);
  }

  getLastOpTimestamp(replSetName) {
    const key = this.getLastOpTimeKey(replSetName);
    return new Promise((resolve, reject) => {
      this.redisClient.get(key, (err, val) => {
        if (err) return reject(err);
        if (!val) return resolve();
        const [unixTimestamp, opSeq] = val.split('|').map(n => Number(n));
        const ts = Timestamp(opSeq, unixTimestamp);
        resolve(ts);
      })
    });
  }

  setConnectionStrings(newConnStrings) {
    // destroy old/unneeded oplog connections
    Object.keys(this.oplogs).forEach(connStr => {
      if (!newConnStrings.includes(connStr)) {
        this.oplogs[connStr].destroy()
          .then(() => {
            console.log('removing oplog:', connStr);
            delete this.oplogs[connStr];
          });
      }
    });
    newConnStrings.forEach(newConnStr => {
      const exists = Object.keys(this.oplogs).includes(newConnStr);
      if (!exists) {
        // new shard!
        // TODO: if this replSet has been seen before, start tailing at lastOpTime
        console.log('new oplog:', newConnStr);
        // tail starting from the beginning of the oplog
        this.tailHost(newConnStr, { since: 1 });
      }
    });
    this.connectionStrings = Object.keys(this.oplogs);
  }

  /**
   * Emit the event only if it has not already been emitted
   * @param data
   * @return {Boolean} whether the event was emitted (false if previously emitted)
   */
  emitEvent(data) {
    return new Promise((resolve, reject) => {
      const opId = this.getOpId(data);
      const key = `${this.keyPrefix}:emittedEvents:${opId}`;
      // check if this event has been emitted already by another process
      this.redisClient.setnx(key, true, (err, notAlreadyEmitted) => {
        if (err) return reject(err);
        const alreadyEmitted = !notAlreadyEmitted;
        if (alreadyEmitted) return resolve(false);
        this.emit('op', data);
        this.redisClient.expire(key, this.ttl, err => {
          if (err) return reject(err);
          return resolve(true);
        });
      });
    });
  }

  getOpId(data) {
    const unixTime = data.ts.high_;
    const opSeq = data.ts.low_;
    return `${unixTime}|${opSeq}|${data.h}`;
  }

  // determine if we've detected this op on the majority of replset members
  isOpMajorityDetected(replSetName, memberName, data) {
    return new Promise((resolve, reject) => {
      const opId = this.getOpId(data);
      const key = `${this.keyPrefix}:op:${opId}`;
      // add this member to the set of members that received this op
      this.redisClient.sadd(key, memberName, (err) => {
        if (err) return reject(err);
        this.redisClient.expire(key, this.ttl, err => {
          if (err) return reject(err);
          // check if this op has been received by the majority of members
          this.redisClient.scard(key, (err, numMembers) => {
            if (err) return reject(err);
            const majority = Math.ceil(Object.keys(this.replSets[replSetName]).length / 2);
            return resolve(numMembers >= majority);
          });
        });
      });
    });
  }

  processOp(data, replSetName, memberName) {
    return this.isOpMajorityDetected(replSetName, memberName, data)
      .then(hasMajority => {
        if (hasMajority) {
          return this.emitEvent(data);
        }
      })
      .then(justEmitted => {
        if (justEmitted) {
          return this.setLastOpTimestamp(replSetName, data);
        }
      });
  }

  // TODO: currently, we assume a correct/healthy replSet state at startup
  setMembersOfReplSet(db) {
    return db.admin().replSetGetStatus()
      .then(info => {
        const replSetName = info.set;
        this.replSets[replSetName] = this.replSets[replSetName] || {};
        info.members.forEach(member => {
          if (!member.health) return; // ignore unhealthy members
          this.replSets[info.set][member.name] = member;
        });
        return info;
      });
  }

  tailHost(connStr, options) {
    MongoDB.MongoClient.connect(connStr).then(db => {
      const oplog = MongoOplog(db, options);
      this.oplogs[connStr] = oplog;
      this.setMembersOfReplSet(db).then(info => {
        const replSetName = info.set;
        const memberName = info.members.find(member => member.self).name;
        oplog.on('op', data => {
          if (data.fromMigrate) return; // ignore shard balancing ops
          if (data.op === 'n') return; // ignore informational no-operation
          this.processOp(data, replSetName, memberName);
          this.emit('shard-op', { data, replSetName, memberName })
        });
        oplog.on('end', () => {
          // TODO: reconnect
          console.log('Stream ended');
        });
        oplog.tail();
      });
    });
  }

}

module.exports = MongoOplogReader;
