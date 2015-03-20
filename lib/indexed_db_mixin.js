/**
 * In addition to standard backbone-db functionality redis-db
 * driver contains functionality for managing indexed collections
 */
var _ = require('lodash');
var utils = require('./utils');
var debug = require('debug')('backbone-db-redis:indexed_db_mixin');
var IndexedDBAdapterInterface = require('backbone-db-indexing-adapter').IndexedDBAdapterInterface;

// Redis-db implements IndexedDBAdapterInterface
module.exports = _.extend({}, IndexedDBAdapterInterface, {
  /**
   * Adds model's key to this index. If index is to be a sorted set, collection should
   * define indexSort function, otherwise store in a set.
   *
   * @param {Collection}  collection  instance of indexed collection
   * @param {Model}       model       Model to be added to index
   * @param {Object}      options     not used currently
   * @param {Function}    cb          Callback function
   */
  addToIndex: function(collection, model, options, cb) {
    var setKey = collection.indexKey;
    var key = model.id;
    debug('adding model ' + key + ' to ' + setKey);
    if (collection.indexSort) {
      this.redis.zadd(setKey, collection.indexSort(model), key, cb);
    } else {
      this.redis.sadd(setKey, key, function(err, res) {
        cb(err, res);
      });
    }
  },

  readFromIndex: function(collection, options, cb) {
    var self = this;
    var setKey = options.indexKey || collection.indexKey;
    var dynamicSorted = false;

    var done = function(err, data) {
      data = data || []; // Data might be null on errors
      var models = [];
      var i = 0;
      while (i < data.length) {
        var modelData = {
          id: data[i]
        };
        i++;
        if (options.score && options.score.conversion) {
          var score = options.score.conversion.fn(data[i]);
          modelData[options.score.conversion.attribute] = score;
          i++;
        }
        models.push(modelData);
      }
      var setOpts = {};
      // disable sort by default here, since it's expected that redis set was sorted
      if (collection.indexSort) setOpts.sort = false;
      var opts = _.extend(setOpts, options);
      if (dynamicSorted) opts.sort = false;
      collection.set(models, opts);
      return cb(err, models);
    };

    var readFromSortedSet = function() {
      var min = '-inf';
      var max = '+inf';
      var params;

      if (options.score) {
        min = options.score.min || min;
        max = options.score.max || max;
        params = [setKey, max, min];
        if (options.score.conversion) params.push('WITHSCORES');
        if (options.limit || options.offset) {
          params = params.concat(['LIMIT', options.offset || 0, options.limit || -1]);
        }
        if (options.sortOrder === 1) {
          return _.bind.apply(null, [self.redis.zrangebyscore, self.redis].concat(params));
        }
        return _.bind.apply(null, [self.redis.zrevrangebyscore, self.redis].concat(params));
      }

      var start = options.offset || 0;
      var stop = options.limit ? (start + options.limit - 1) : -1;
      if (options.sortOrder === 1) {
        return _.bind(self.redis.zrange, self.redis, setKey, start, stop);
      }
      return _.bind(self.redis.zrevrange, self.redis, setKey, start, stop);
    };

    var sortUsingHashField = function() {
      dynamicSorted = true;
      var m = new collection.model();
      var idKey = self.getIdKey(m);
      var parsedSort = utils.parseSort(options.sort);
      var sortParams = idKey + ':*->' + parsedSort.sortProp;
      var params = [setKey, 'BY', sortParams];

      if (options.limit_query !== false && (options.limit || options.offset)) {
        params = params.concat([
          'LIMIT',
          options.offset || 0,
          options.limit || -1
        ]);
      }

      if (parsedSort.sortOrder === -1) {
        params.push('DESC');
      }
      debug('dynamic sort:', params);
      return _.bind.apply(null, [self.redis.sort, self.redis].concat(params));
    };

    var getReadFn = function() {
      if (collection.indexSort) {
        return readFromSortedSet();
      }
      if (options.sort && collection.model.prototype.redis_type === 'hash') {
        return sortUsingHashField();
      }

      return _.bind(self.redis.smembers, self.redis, setKey);
    };

    // TODO: handle sort order
    var readWithRank = function(_done) {
      debug('readWithRank');
      if (!collection.indexSort) throw new Error('Cannot read rank of non-sorted set');
      var id = options.before_id ? options.before_id : options.after_id;
      // by default order set descending
      var order = options.sortOrder ? options.sortOrder : -1;
      var rankFn = 'zrank';
      var rangeFn = 'zrange';
      var start;
      var stop;
      if (order === -1) {
        rankFn = 'zrevrank';
        rangeFn = 'zrevrange';
      }
      // first: read rank for given id
      self.redis[rankFn](setKey, id, function(err, rank) {
        // debug('got rank: %s for id: %s, using %s %s', rank, id, rankFn, rangeFn);
        if (options.after_id) {
          start = rank + 1;
          stop = options.limit
            ? (start + options.limit - 1)
            : -1;

        } else if (options.before_id) {
          if (rank === 0) {
            // there`s nothing before given id
            return _done(null, []);
          }
          if (order === 1 && options.limit) {
            start = rank - 1;
          } else {
            start = 0;
          }
          stop = rank - 1;
          if (options.limit) stop = start - 1 + options.limit;
        }

        var params = [setKey, start, stop];
        if (options.score && options.score.conversion) params.push('WITHSCORES');
        params.push(_done);

        // second: read results with zrevrange or zrange
        self.redis[rangeFn].apply(self.redis, params);
      });
    };

    debug('reading keys from: ' + setKey);
    if (options.before_id || options.after_id) {
      return readWithRank(done);
    }
    var readFn = getReadFn();
    readFn(done);
  },

  /*
   * Read from multiple sets, storing union/intersection in new set temporarily
   * unionKey or intersectionKey should be given in options
   */
  readFromIndexes: function(collection, options, cb) {
    var self = this;
    var unionKey = options.unionKey;
    var intersectionKey = options.intersectionKey;
    var expirationTime = options.expirationTime || 300;
    var params = _.clone(options.indexKeys);
    if (collection.indexSort) {
      params.unshift(options.indexKeys.length); // how many sets to union
    }
    params.unshift(unionKey || intersectionKey); // where to store
    if (options.weights) {
      params.push('WEIGHTS');
      params.push.apply(params, options.weights);
    }
    // by default use MAX aggregate option (redis default is SUM)
    if (collection.indexSort) {
      params.push('AGGREGATE', 'MAX');
    }

    var fn;
    if (unionKey) {
      fn = collection.indexSort
        ? _.bind(this.redis.zunionstore, this.redis)
        : _.bind(this.redis.sunionstore, this.redis);
    } else if (intersectionKey) {
      fn = collection.indexSort
        ? _.bind(this.redis.zinterstore, this.redis)
        : _.bind(this.redis.sinterstore, this.redis);
    }
    if (!fn) {
      throw new Error('unionKey or intersectionKey must be given');
    }

    fn(params, function(err) {
      if (err) return cb(err);
      self.redis.expire(unionKey, expirationTime);
      options.indexKey = unionKey || intersectionKey;
      return self.readFromIndex(collection, _.extend({limit_query: false}, options), cb);
    });
  },

  removeFromIndex: function(collection, models, options, cb) {
    var setKey = collection.indexKey;
    var keys = _.pluck(models, models[0].idAttribute);
    var cmd = [setKey].concat(keys);
    debug('removing key: ' + keys + ' from: ' + setKey);
    if (collection.indexSort) {
      this.redis.zrem(cmd, cb);
    } else {
      this.redis.srem(cmd, cb);
    }
  },

  // removes the index completely
  removeIndex: function(collection, options, cb) {
    var setKey = collection.indexKey;
    this.redis.del(setKey, cb);
  },

  existsInIndex: function(collection, model, options, cb) {
    var setKey = collection.indexKey;
    var key = model.id;
    debug('check existance for: ' + key + ' in: ' + setKey);

    function done(err, rank) {
      cb(err, rank !== null);
    }

    if (collection.indexSort) {
      this.redis.zrank(setKey, key, done);
    } else {
      this.redis.sismember(setKey, key, function(err, isMember) {
        cb(err, isMember === 1);
      });
    }
  },

  /**
   * Get number of items in the index
   * @param {Collection}  collection  instance of indexed collection
   * @param {Object}      options     not used currently
   * @param {Function}    cb          Callback function
   */
  indexCount: function(collection, options, cb) {
    var setKey = collection.indexKey;
    debug('get count for: ' + setKey);
    if (collection.indexSort) {
      this.redis.zcard(setKey, cb);
    } else {
      this.redis.scard(setKey, cb);
    }
  },

  /**
   * Return the score of model in this index
   * @param {Collection}  collection  instance of indexed collection,
   *                                  should have indexSort defined
   * @param {Model}       model       Model whose score is to be fetched
   * @param {Object}      options     not used currently
   * @param {Function}    cb          Callback function
   */
  score: function(collection, model, options, cb) {
    if (!collection.indexSort) {
      throw new Error('Cannot read score with non-sorted set');
    }
    var setKey = collection.indexKey;
    var member = model.id;
    this.redis.zscore(setKey, member, cb);
  }
});
