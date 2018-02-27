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
const Debug = require('debug');

const Timestamp = MongoDB.Timestamp;
const debug = Debug('MongoOplogReader');

const defaults = {
  keyPrefix: 'mongoOplogReader',
  ttl: 60 * 60 * 24, // amount of time in seconds to keep track of the emit status of an op
  redundancy: 1,
  masterDuration: 30, // in seconds
  healthcheckDuration: 10 // in seconds
};

const defaultMongoConnectOptions = {
  server: {
    poolSize: 100,
    auto_reconnect: true,
    socketOptions: {
      keepAlive: 5000,
      connectTimeoutMS: 30000,
      socketTimeout: 30000
    }
  }
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
    this.mongoConnectOptions = options.mongoConnectOptions || defaultMongoConnectOptions;
    this.workerId = `${Math.random()}`.substr(2);
    this.workerRegistrationKey = `${this.keyPrefix}:workerIds`;
    this.assignmentsByConnStr = {};
    this.assignmentsByWorkerId = {};

    if (!Number.isInteger(this.redundancy) || this.redundancy > 10) {
      throw new Error(`Specified redundancy '${this.redundancy}' should be an integer less than 10.`);
    }
  }

  start() {
    if (!Array.isArray(this.connectionStrings) || !this.connectionStrings.length) {
      throw new Error('There are no connectionStrings.');
    }
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
        debug(`${this.workerId} is master.`);
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
    debug(`availableWorkerId: ${availableWorkerId}`);
    return availableWorkerId;
  }

  populateWorkerAssignments() {
    this.assignmentsByConnStr = {};
    this.assignmentsByWorkerId = {};
    return this.getWorkerIds().then(workerIds => {
      return Promise.map(workerIds, workerId => {
        return this.readWorkerAssignmentsFromRedis(workerId).then(connStrs => {
          connStrs.forEach(connStr => this.recordAssignment(workerId, connStr));
        });
      });
    });
  }

  assignMoreWorkersIfNecessary() {
    return Promise.map(this.connectionStrings, connStr => {
      let workers = [];
      while (workers.length < this.redundancy) {
        const workerId = this.getAvailableWorkerId();
        // prevent infinite loop if there are no available workerIds
        if (!workerId) {
          break;
        }
        this.recordAssignment(workerId, connStr);
        workers = this.assignmentsByConnStr[connStr] || [];
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
        debug(`workerId[${this.workerId}] assignments: %o`, connStrs);
        connStrs.forEach(connStr => {
          if (inProgressConnStrs.includes(connStr)) {
            return;
          }
          this.tailHost(connStr);
        });
      });
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
    this.connectionStrings = newConnStrings;
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
    debug(`processOp: ${replSetName} ${memberName} %o`, data);
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
    return db.command({ 'isMaster': 1 })
      .then(info => {
        const replSetName = info.setName;
        this.replSets[replSetName] = this.replSets[replSetName] || {};
        info.hosts.forEach(host => {
          this.replSets[replSetName][host] = true;
        });
        debug('replSets: %o', this.replSets);
        return info;
      });
  }

  tailHost(connStr) {
    debug(`tailHost: ${connStr}`);
    return MongoDB.MongoClient.connect(connStr, this.mongoConnectOptions).then(db => {
      // We may need this code to reconnect, need to test auto_reconnect
      // db.on('close', err => {
      //   console.log(connStr, err);
      //   this.tailHost(connStr);
      // });
      // db.on('error', err => {
      //   console.log(connStr, err);
      //   this.tailHost(connStr);
      // });
      return this.readFromOplog(connStr, db);
    });
  }

  readFromOplog(connStr, db) {
    debug(`readFromOplog: ${connStr}`);
    return this.setMembersOfReplSet(db).then(info => {
      debug('replSet info: %o', info);
      const replSetName = info.setName;
      const memberName = info.me;
      return this.getLastOpTimestamp(replSetName).then(ts => {
        debug(`${replSetName} ts: %s`, ts);
        const opts = { since: ts || 1 }; // start where we left off, otherwise from the beginning
        const oplog = MongoOplog(db, opts);
        this.oplogs[connStr] = oplog;
        oplog.on('op', data => {
          if (data.fromMigrate) return; // ignore shard balancing ops
          if (data.op === 'n') return; // ignore informational no-operation
          this.processOp(data, replSetName, memberName);
          this.emit('shard-op', { data, replSetName, memberName });
        });
        oplog.on('end', () => {
          // oplog stream ended, pick up where we left off with the same db connection
          this.readFromOplog(connStr, db);
        });
        oplog.tail();
      });
    });
  }

}

module.exports = MongoOplogReader;
