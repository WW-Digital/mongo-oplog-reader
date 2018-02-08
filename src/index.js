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
const Promise = require('bluebird');
const RedisLock = require('redislock');

const Timestamp = MongoDB.Timestamp;

const defaults = {
  keyPrefix: 'mongoOplogReader',
  ttl: 60 * 60 * 24, // amount of time to keep track of the emit status of an op
  redundancy: 1,
  masterDuration: 30,
  healthcheckDuration: 10
};

class MongoOplogReader extends EventEmitter {

  /**
   * Create a new MongoOplogReader
   * @param options
   *  - connectionStrings {String[]} a list of mongodb connection strings
   *  - redisClient {Object} an instance of the 'redis' module
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
      throw new Error('Invalid redisClient.');
    }
    this.connectionStrings = options.connectionStrings;
    this.redisClient = Promise.promisifyAll(options.redisClient);
    this.lock = RedisLock.createLock(options.redisClient);
    this.oplogs = {};
    this.replSets = {};
    this.redundancy = options.redundancy || defaults.redundancy;
    this.keyPrefix = options.keyPrefix || defaults.keyPrefix;
    this.ttl = options.ttl || defaults.ttl;
    this.masterDuration = options.masterDuration || defaults.masterDuration;
    this.healthcheckDuration = options.healthcheckDuration || defaults.healthcheckDuration;
    this.workerId = `${Math.random()}`.substr(2);
    this.workerRegistrationKey = `${this.keyPrefix}:workerIds`;
    this.assignmentsByConnStr = {};
    this.assignmentsByWorkerId = {};
  }

  start() {
    return Promise.resolve()
      .then(() => this.registerWorker())
      .then(() => this.acquireMasterLock())
      .then(() => this.startPolling());
  }

  startPolling() {
    // try to acquire the master lock every 15-30 seconds by default
    const masterMs = this.masterDuration * 500;
    this.masterInterval = setInterval(() => {
      const randomDelay = Math.floor(masterMs * Math.random());
      setTimeout(() => this.acquireMasterLock(), randomDelay);
    }, masterMs);

    // re-register this worker every 10 seconds by default
    const healthcheckMs = this.healthcheckDuration * 1000;
    this.healthcheckInterval = setInterval(() => this.registerWorker(), healthcheckMs);
  }

  /**
   * If the "master" lock is acquired, this worker will be responsible for
   * assigning oplogs to workers
   */
  acquireMasterLock() {
    const key = `${this.keyPrefix}:master`;
    return this.lock.acquire(key, {
      timeout: this.masterDuration * 1000,
      retries: 0
    })
      .then(() => {
        console.log(this.workerId, 'is master.');
        return this.assignWorkers();
      })
      // ignore lock acquisition errors
      .catch(err => {});
  }

  registerWorker() {
    const key = this.workerRegistrationKey;
    const now = Date.now();
    return this.redisClient.zaddAsync(key, now, this.workerId)
      .then(() => this.startNewAssignments())
  }

  getWorkerIds() {
    const expiredTime = Date.now() - (this.healthcheckDuration * 1000);
    const key = this.workerRegistrationKey;
    return Promise.resolve()
      // delete workers that have failed to renew their registration
      .then(() => this.redisClient.zremrangebyscoreAsync(key, '-inf', expiredTime))
      // get active workers
      .then(() => this.redisClient.zrangeAsync(key, 0, -1));
  }

  getWorkerAssignmentsKey(workerId) {
    return `${this.keyPrefix}:worker:${workerId}`;
  }

  readWorkerAssignmentsFromRedis(workerId) {
    const key = this.getWorkerAssignmentsKey(workerId);
    return this.redisClient.smembersAsync(key);
  }

  writeWorkerAssignmentsToRedis() {
    return Promise.map(Object.keys(this.assignmentsByWorkerId), workerId => {
      const key = this.getWorkerAssignmentsKey(workerId);
      const connStrs = this.assignmentsByWorkerId[workerId];
      console.log('write', key, connStrs);
      return this.redisClient.saddAsync(key, connStrs)
        .then(() => this.redisClient.expireAsync(key, this.masterDuration * 60));
    });
  }

  recordAssignment(workerId, connStr) {
    this.assignmentsByConnStr[connStr] = this.assignmentsByConnStr[connStr] || [];
    this.assignmentsByConnStr[connStr].push(workerId);
    this.assignmentsByWorkerId[workerId] = this.assignmentsByWorkerId[workerId] || [];
    this.assignmentsByWorkerId[workerId].push(connStr);
  }

  getAvailableWorkerId() {
    let availableWorkerId = this.workerId;
    Object.keys(this.assignmentsByWorkerId).forEach(workerId => {
      if (this.assignmentsByWorkerId[workerId].length < this.assignmentsByWorkerId[availableWorkerId]) {
        availableWorkerId = workerId;
      }
    });
    return availableWorkerId;
  }

  populateWorkerAssignments() {
    this.assignmentsByConnStr = {};
    this.assignmentsByWorkerId = {};
    return this.getWorkerIds()
      .then(workerIds => {
        return Promise.map(workerIds, workerId => {
          return this.readWorkerAssignmentsFromRedis(workerId)
            .then(connStrs => {
              connStrs.forEach(connStr => this.recordAssignment(workerId, connStr));
            });
        });
      });
  }

  assignMoreWorkersIfNecessary() {
    return Promise.map(this.connectionStrings, connStr => {
      while (true) {
        const workers = this.assignmentsByConnStr[connStr] || [];
        if (workers.length >= this.redundancy) {
          break;
        }
        const workerId = this.getAvailableWorkerId();
        this.recordAssignment(workerId, connStr);
      }
    });
  }

  assignWorkers() {
    return Promise.resolve()
      .then(() => this.populateWorkerAssignments())
      .then(() => this.assignMoreWorkersIfNecessary())
      .then(() => this.writeWorkerAssignmentsToRedis())
      .then(() => this.startNewAssignments())
  }

  startNewAssignments() {
    const inProgressConnStrs = Object.keys(this.oplogs);
    return this.readWorkerAssignmentsFromRedis(this.workerId)
      .then(connStrs => {
        connStrs.forEach(connStr => {
          if (inProgressConnStrs.includes(connStr)) {
            return;
          }
          this.tailHost(connStr);
        });
      });
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
    return this.redisClient.setAsync(key, val);
  }

  getLastOpTimestamp(replSetName) {
    const key = this.getLastOpTimeKey(replSetName);
    return this.redisClient.getAsync(key).then(val => {
      if (!val) return null;
      const [unixTimestamp, opSeq] = val.split('|').map(n => Number(n));
      return Timestamp(opSeq, unixTimestamp);
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
    const opId = this.getOpId(data);
    const key = `${this.keyPrefix}:emittedEvents:${opId}`;
    // check if this event has been emitted already by another process
    return this.redisClient.setnxAsync(key, true).then(notAlreadyEmitted => {
      const alreadyEmitted = !notAlreadyEmitted;
      if (alreadyEmitted) return false;
      this.emit('op', data);
      return this.redisClient.expireAsync(key, this.ttl).then(() => true);
    });
  }

  getOpId(data) {
    const unixTime = data.ts.high_;
    const opSeq = data.ts.low_;
    return `${unixTime}|${opSeq}|${data.h}`;
  }

  // determine if we've detected this op on the majority of replset members
  isOpMajorityDetected(replSetName, memberName, data) {
    const opId = this.getOpId(data);
    const key = `${this.keyPrefix}:op:${opId}`;
    // add this member to the set of members that received this op
    return this.redisClient.saddAsync(key, memberName)
      .then(() => this.redisClient.expireAsync(key, this.ttl))
      // check if this op has been received by the majority of members
      .then(() => this.redisClient.scardAsync(key))
      .then(numMembers => {
        const majority = Math.ceil(Object.keys(this.replSets[replSetName]).length / 2);
        return numMembers >= majority;
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
    return MongoDB.MongoClient.connect(connStr).then(db => {
      const oplog = MongoOplog(db, options);
      console.log('new oplog', connStr);
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
