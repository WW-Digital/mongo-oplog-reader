/**
 * High availability tailing mongo oplogs of a sharded cluster
 *
 * - Will emit events only after majority write
 * - Will emit events only once even if multiple processes are running
 * - Will resume tailing after the last emitted op
 * - Ability to accept a new set of oplog hosts (i.e. if a shard is added)
 *
 * TODO:
 *  - Add TTL for redis keys
 *
 * More info:
 *  - https://www.mongodb.com/blog/post/tailing-mongodb-oplog-sharded-clusters
 *  - https://www.mongodb.com/blog/post/pitfalls-and-workarounds-for-tailing-the-oplog-on-a-mongodb-sharded-cluster
 *
 */

const MongoDB = require('mongodb');
const MongoOplog = require('mongo-oplog');
const EventEmitter = require('eventemitter3');

const Timestamp = MongoDB.Timestamp;
const defaultKeyPrefix = 'mongoOplogReader';

class MongoOplogReader extends EventEmitter {

  /**
   * Instantiate a new MongoOplogReader
   * @param options
   *  - connectionStrings {String[]} a list of mongodb connection strings
   *  - redisClient {Object}
   *  - keyPrefix {String} the redis key prefix (default: mongoOplogReader)
   */
  constructor(options) {
    super();
    if (!Array.isArray(options.connectionStrings) || !options.connectionStrings.length) {
      throw new Error('connectionStrings is required.');
    }
    if (!options.redisClient || !options.redisClient.sadd) {
      throw new Error('redisClient must support GET,SET,SADD,SCARD commands.');
    }
    this.connectionStrings = options.connectionStrings;
    this.redisClient = options.redisClient;
    this.oplogs = {};
    this.replSets = {};
    this.keyPrefix = options.keyPrefix || defaultKeyPrefix;
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
    return `${this.keyPrefix}:${replSetName}:lastOpTime`;
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
        // start reading the oplog of a new shard
        console.log('new oplog:', newConnStr);
        // TODO: do we need to start the tail at a time in the past?
        this.tailHost(newConnStr);
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
      const key = `${this.keyPrefix}:emittedEvents`;
      // check if this event has been emitted already by another process
      this.redisClient.sadd(key, opId, (err, notAlreadyEmitted) => {
        if (err) return reject(err);
        const alreadyEmitted = !notAlreadyEmitted;
        if (alreadyEmitted) return resolve(false);
        this.emit('op', data);
        return resolve(true);
      });
    });
  }

  getOpId(data) {
    return `${data.ts}_${data.h}`;
  }

  // determine if we've detected this op on the majority of replset members
  isOpMajorityDetected(replSetName, memberName, data) {
    return new Promise((resolve, reject) => {
      const opId = this.getOpId(data);
      const key = `${this.keyPrefix}:op:${opId}`;
      // add this member to the set of members that received this op
      this.redisClient.sadd(key, memberName, (err) => {
        if (err) return reject(err);
        // check if this op has been received by the majority of members
        this.redisClient.scard(key, (err, numMembers) => {
          if (err) return reject(err);
          const majority = Math.ceil(Object.keys(this.replSets[replSetName]).length / 2);
          return resolve(numMembers >= majority);
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

  setMembersOfReplSet(db) {
    return db.admin().replSetGetStatus()
      .then(info => {
        const replSetName = info.set;
        this.replSets[replSetName] = this.replSets[replSetName] || {};
        info.members.forEach(member => {
          if (!member.health) return;
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
        });
        oplog.on('end', () => {
          console.log('Stream ended');
        });
        oplog.tail();
      });
    });
  }

}

module.exports = MongoOplogReader;
