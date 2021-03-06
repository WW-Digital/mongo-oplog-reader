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
const createOplogStream = require('./createOplogStream');
const Promise = require('bluebird');
const RedisLock = require('redislock');
const Debug = require('debug');
const through2Concurrent = require('through2-concurrent');

const Timestamp = MongoDB.Timestamp;
const debug = Debug('MongoOplogReader');

const defaults = {
  keyPrefix: 'mongoOplogReader',
  ttl: 60 * 60 * 24, // amount of time in seconds to keep track of the emit status of an op
  workersPerOplog: 1, // total # of redundant workers per oplog (respected across all processes)
  maxConcurrencyPerWorker: 10, // max # of oplog events to be processed concurrently per worker
  masterDuration: 30, // in seconds
  healthcheckDuration: 10 // in seconds
};

const defaultMongoConnectOptions = {
  poolSize: 100,
  autoReconnect: true,
  keepAlive: 5000,
  connectTimeoutMS: 30000,
  socketTimeoutMS: 30000,
  useNewUrlParser: true
};

const opCode = {
  COMMAND: 'c',
  DELETE: 'd',
  INSERT: 'i',
  NOOP: 'n',
  UPDATE: 'u'
};

class MongoOplogReader {

  /**
   * Create a new MongoOplogReader
   * @param options
   *  - connectionStrings {String[]} a list of mongodb connection strings
   *  - redisClient {Object} an instance of the 'redis' module
   *  - workersPerOplog {Integer} number of workers per oplog (for redundancy purposes)
   *  - maxConcurrencyPerWorker {Integer} number of concurrent oplog events processed per worker
   *  - ttl {Integer} max time it may take a cluster to recover (in seconds)
   *  - keyPrefix {String} the redis key prefix (default: 'mongoOplogReader')
   */
  constructor(options) {
    if (!options.redisClient || !options.redisClient.sadd) {
      throw new Error('Invalid redisClient.');
    }
    this.connectionStrings = options.connectionStrings;
    this.redisClient = Promise.promisifyAll(options.redisClient);
    this.lock = RedisLock.createLock(options.redisClient);
    this.oplogs = {};
    this.replSets = {};
    this.workersPerOplog = options.workersPerOplog || defaults.workersPerOplog;
    this.maxConcurrencyPerWorker = options.maxConcurrencyPerWorker || defaults.maxConcurrencyPerWorker;
    this.keyPrefix = options.keyPrefix || defaults.keyPrefix;
    this.ttl = options.ttl || defaults.ttl;
    this.masterDuration = options.masterDuration || defaults.masterDuration;
    this.healthcheckDuration = options.healthcheckDuration || defaults.healthcheckDuration;
    this.mongoConnectOptions = options.mongoConnectOptions || defaultMongoConnectOptions;
    this.startAt = options.startAt;
    this.workerId = `${Math.random()}`.substr(2);
    this.workerRegistrationKey = `${this.keyPrefix}:workerIds`;
    this.assignmentsByConnStr = {};
    this.assignmentsByWorkerId = {};
    this.eventHandlers = [];
    this.throttleDelay = 0;
    this.throttleMemUsageThreshold = options.throttleMemUsageThreshold; // decimal percent format, i.e. 0.85
    this.filterOp = () => true; // don't filter any ops by default

    if (!Number.isInteger(this.workersPerOplog) || this.workersPerOplog > 10 || this.workersPerOplog < 1) {
      throw new Error(`workersPerOplog '${this.workersPerOplog}' must be an integer between 1 and 10.`);
    }
  }

  filter(fn) {
    this.filterOp = fn;
  }

  start() {
    if (!Array.isArray(this.connectionStrings) || !this.connectionStrings.length) {
      throw new Error('There are no connectionStrings.');
    }
    return Promise.resolve()
      .then(() => this.registerWorker())
      .then(() => this.acquireMasterLock())
      .then(() => this.setThrottleDelay())
      .then(() => this.startPolling());
  }

  onEvent(fn) {
    this.eventHandlers.push(fn);
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

    // adjust throttling every 10 seconds
    this.throttleAdjustmentInterval = setInterval(() => this.setThrottleDelay(), 10000);
  }

  /**
   * Slow down the rate of processing the oplog if redis memory usage is high
   */
  setThrottleDelay() {
    if (!this.throttleMemUsageThreshold) {
      return;
    }
    return this.redisClient.infoAsync('memory')
      .then(info => {
        const usedMem = info.match(/used_memory:(\d*)/)[1];
        const totalMem = info.match(/total_system_memory:(\d*)/)[1];
        const maxMem = info.match(/maxmemory:(\d*)/)[1];
        const memUsage = usedMem / (maxMem || totalMem);
        /**
         * Throttle the throughput based on redis memory usage. For example, assuming 24 hour TTL and 75% threshold:
         * Redis usage:  |  Delay:       
         *           0%  |  none
         *          50%  |  none
         *          75%  |  none
         *        75.1%  |  ~5 minutes
         *          76%  |  ~1 hour
         *         100%  |  ~24 hours
         */
        const memThreshold = this.throttleMemUsageThreshold;
        this.throttleDelay = Math.floor(this.ttl * Math.max(memUsage - memThreshold, 0) / (1 - memThreshold));
        debug('Redis Memory Usage:', usedMem, '/', totalMem, '=', memUsage);
        debug('throttleDelay:', this.throttleDelay, 'seconds');
      });
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
        // If a worker crashes, then the assignments for that workerId will be orphaned.
        // This expiration will clear orphaned assignments after 2-5 minutes (by default)
        .then(() => this.redisClient.expireAsync(key, this.masterDuration * 10));
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
      while (workers.length < this.workersPerOplog) {
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
    const key = `${this.keyPrefix}:emit:${opId}`;
    // check if this event has been emitted already by another process
    return this.redisClient.setnxAsync(key, 1).then(notAlreadyEmitted => {
      const alreadyEmitted = !notAlreadyEmitted;
      if (alreadyEmitted) return false;
      return Promise.resolve()
        .then(() => this.redisClient.expireAsync(key, this.ttl))
        .then(() => Promise.map(this.eventHandlers, eventHandler => eventHandler(data)))
        .then(() => true);
    });
  }

  getOpId(data) {
    const unixTime = data.ts.high_;
    const opSeq = data.ts.low_;
    return `${unixTime}|${opSeq}|${data.h}`;
  }

  getOpMembersKey(opId) {
    return `${this.keyPrefix}:op:${opId}`;
  }

  // determine if we've detected this op on the majority of replset members
  isOpMajorityDetected(replSetName, memberName, opId) {
    const key = this.getOpMembersKey(opId);
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

  deleteOpMembers(opId) {
    // wait a few seconds to prevent redundant workers from recreating the key immediately after it's deleted
    // if a redundant worker is way slower, this key will be purged passively after the TTL
    setTimeout(() => {
      const key = this.getOpMembersKey(opId);
      this.redisClient.delAsync(key);
    }, 3000);
  }

  processOp(data, replSetName, memberName) {
    const skip = !this.filterOp(data);
    if (skip) return Promise.resolve();
    debug(`processOp: ${replSetName} ${memberName} %o`, data);
    const opId = this.getOpId(data);
    return this.isOpMajorityDetected(replSetName, memberName, opId)
      .then(hasMajority => {
        if (!hasMajority) return;
        return this.emitEvent(data)
          .then(justEmitted => {
            // event has been emitted, delete the OpMembers key in the background to free up redis memory
            this.deleteOpMembers(opId);
            if (justEmitted) {
              return this.setLastOpTimestamp(replSetName, data);
            }
          });
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
    const client = new MongoDB.MongoClient(connStr, this.mongoConnectOptions);
    client.connect().then(() => {
      const db = client.db();
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
      const startTs = this.startAt ? Promise.resolve(this.startAt) : this.getLastOpTimestamp(replSetName);
      return startTs.then(ts => {
        debug(`${replSetName} ts: %s`, ts);
        const opts = { startAt: ts || 1 }; // start where we left off, otherwise from the beginning
        const stream = createOplogStream(db, opts);
        const maxConcurrency = this.maxConcurrencyPerWorker;
        this.oplogs[connStr] = stream;
        stream.pipe(through2Concurrent.obj({ maxConcurrency }, (data, enc, done) => {
          Promise.delay(this.throttleDelay * 1000)
            .then(() => {
              if (data.fromMigrate) return done(); // ignore shard balancing ops
              if (data.op === opCode.NOOP) return done(); // ignore informational no-operation
              return this.processOp(data, replSetName, memberName)
                .then(() => done())
                .catch(err => done(err));
            });
        }));
        stream.on('end', () => {
          // oplog stream ended, pick up where we left off with the same db connection
          console.log('stream ended');
          this.readFromOplog(connStr, db);
        });
      });
    });
  }

}

module.exports = MongoOplogReader;
