const { Timestamp } = require('mongodb');

function getTs(startAt) {
  if (startAt && typeof startAt === 'number') return Timestamp(0, startAt);
  if (startAt) return startAt;
  return Timestamp(0, Date.now() / 1000 | 0);
}

/**
 * Create a mongo oplog stream
 * @param db
 * @param options
 *   - startAt {Integer|} Oplog Timestamp to begin streaming from, otherwise stream new event starting now
 * @return stream
 */
function createOplogStream(db, options) {
  if (!db) throw new Error('Mongo db is required');

  const query = {
    ts: { $gt: getTs(options.startAt) }
  };

  const streamOptions = {
    tailable: true,
    awaitData: true,
    oplogReplay: true,
    noCursorTimeout: true,
    numberOfRetries: Number.MAX_VALUE
  };

  return db.collection('oplog.rs').find(query, streamOptions).stream();
}

module.exports = createOplogStream;
