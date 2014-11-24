var _ = require('lodash'),
  Backbone = require('backbone'),
  Db = require('backbone-db'),
  redis = require('redis'),
  debug = require('debug')('backbone-db-redis'),
  indexing = require('./lib/indexing'),
  query = require('./lib/query'),
  utils = require('./lib/utils');


var RedisDb = Backbone.RedisDb = function(name, client) {
  this.name = name ||  '';
  this.redis = client;
  if (!this.redis) {
    this.redis = redis.createClient();
  }
};

Backbone.RedisDb.prototype.key = function(key) {
  if (this.name === '') {
    return key;
  } else {
    return this.name + ':' + key;
  }
};

var loadFnMap = {
  'string': 'get',
  'hash': 'hgetall'
};

var saveFnMap = {
  'string': 'set',
  'hash': 'hmset'
};

var incFnMap = {
  'hash': 'hincrby'
};

_.extend(RedisDb.prototype, Db.prototype, {
  createClient: function() {
    if (this.redis) {
      return redis.createClient(this.redis.port, this.redis.host);
    }
  },

  _getLoadFn: function(model, options) {
    var type = model.redis_type || 'string';
    return loadFnMap[type.toLowerCase()] || 'get';
  },
  _getSaveFn: function(model, options) {
    var type = model.redis_type || 'string';
    return saveFnMap[type.toLowerCase()] || 'set';
  },
  _getIncFn: function(model, options) {
    var type = model.redis_type || 'string';
    if (!incFnMap.hasOwnProperty(type)) {
      throw new Error('Cannot inc with type: ' + type);
    }
    return incFnMap[type.toLowerCase()];
  },
  _getSaveArgs: function(model, options, fn) {
    var args = [this.getIdKey(model, options)];
    options = options || {};
    if (fn === 'hmset') {
      var data = model.toJSON();
      var out = {};
      Object.keys(data).forEach(function(attr) {
        out[attr] = JSON.stringify(data[attr]);
      });
      args.push(out);
    } else if (fn === 'set') {
      args.push(JSON.stringify(model));
    }
    return args;
  },
  _getLoadArgs: function(model, options) {
    var args = [this.getIdKey(model, options)];
    options = options || {};
    return args;
  },
  getFetchCommand: function(model, options) {
    var fn = this._getLoadFn(model);
    var res = {};
    res.args = this._getLoadArgs(model, options, fn);
    res.fn = fn;
    return res;
  },
  getSaveCommand: function(model, options) {
    var fn = this._getSaveFn(model);
    var res = {};
    res.args = this._getSaveArgs(model, options, fn);
    res.fn = fn;
    return res;
  },

  // get key for set where ids are stored
  getIdKey: function(model, options) {
    var key = '';
    options = options || {};
    if (options.url) {
      key = typeof options.url === 'function' ? options.url() : options.url;
    } else if (model.url) {
      key = typeof model.url === 'function' ? model.url() : model.url;
    } else if (model.id) {
      key = model.id;
    }
    if (this.name !== '') {
      return this.name + (key ? ':' + key : '');
    } else {
      return key;
    }
  },

  // get Redis key for set, where key/value is stored
  getValueSetKey: function(model, key, val) {
    var baseKey = model.dbBaseKey || model.type;
    var setKey = 'i:' + baseKey + ':' + key + ':' + val;
    if (this.name !== '') {
      return this.name + ':' + setKey;
    } else {
      return setKey;
    }
  },

  // get Redis key for set for sorted property
  getSortSetKey: function(model, sortProp) {
    var baseKey = model.dbBaseKey || model.type;
    var setKey = 'i:' + baseKey + ':' + sortProp;
    if (this.name !== '') {
      return this.name + ':' + setKey;
    } else {
      return setKey;
    }
  },

  findAll: function(model, options, callback) {
    options = options || {};
    var collectionKey = this.getIdKey(model, options);
    var modelKey;
    var dbOpts;

    debug('findAll ' + collectionKey);
    // if Collection
    if (model.model) {
      var m = new model.model();
      modelKey = this.getIdKey(m, {});
      dbOpts = {
        db: this,
        model: model,
        ModelClass: model.model,
        modelKey: modelKey,
        collectionKey: collectionKey,
        indexes: m.indexes,
        redis_type: m.redis_type
      };
      query.queryModels(options, dbOpts, callback);
    } else {
      // fetch a model with given attributes
      // check that initial attributes are indexed
      var indexedKeys = _.chain(model.indexes)
        .where({unique: true})
        .pluck('property')
        .value();
      var objectKeys = Object.keys(model.attributes);
      var searchAttrs = {};
      var allIndexed = _.each(objectKeys, function(attr) {
        if (indexedKeys.indexOf(attr) > -1) {
          searchAttrs[attr] = model.get(attr);
        }
      });
      if (!Object.keys(searchAttrs).length) {
        var err = new Error('Cannot fetch model with given attributes');
        return callback(err);
      }
      options.where = searchAttrs;
      debug('fetch model', collectionKey, options.where);
      modelKey = this.getIdKey(model, {});
      dbOpts = {
        db: this,
        model: model,
        ModelClass: model.constructor,
        modelKey: modelKey,
        collectionKey: collectionKey,
        indexes: model.indexes,
        redis_type: model.redis_type
      };
      query.queryModels(options, dbOpts, function(err, results) {
        callback(err, results && results.length && results[0]);
      });
    }
  },

  find: function(model, options, callback) {
    var key = this.getIdKey(model, options);
    debug('find: ' + key);
    var cmd = this.getFetchCommand(model, options);
    cmd.args.push(function(err, data) {
      if (typeof data === 'string') {
        data = data && JSON.parse(data);
      } else if (data) {
        _.each(data, function(v, k) {
          try {
            data[k] = JSON.parse(v);
          } catch (e) {
            data[k] = v;
          }
        });
      }
      callback(err, data);
    });
    this.redis[cmd.fn].apply(this.redis, cmd.args);
  },

  create: function(model, options, callback) {
    var self = this;
    var key = this.getIdKey(model, options);
    debug('create: ' + key);
    if (model.isNew()) {
      self.createId(model, options, function(err, id) {
        if (err || !id) {
          return callback(err || new Error('id is missing'));
        }
        model.set(model.idAttribute, id);
        self.update(model, options, callback);
      });
    } else {
      self.update(model, options, callback);
    }
  },

  createId: function(model, options, callback) {
    if (model.createId) return model.createId(callback);
    var key = this.getIdKey(model, options);
    key += ':ids';
    this.redis.incr(key, callback);
  },

  update: function(model, options, callback) {
    if (options.inc) {
      return this.inc(model, options, callback);
    }
    var key = this.getIdKey(model, options);
    var self = this;
    debug('update: ' + key);
    if (model.isNew()) {
      return this.create(model, options, callback);
    }
    var cmd = this.getSaveCommand(model, options);
    cmd.args.push(function(err, res) {
      if (model.collection) {
        var setKey = self.getIdKey(model.collection, {});
        var modelKey = model.get(model.idAttribute);
        debug('adding model ' + modelKey + ' to ' + setKey);
        self.redis.sadd(setKey, modelKey, function(err, res) {
          self._updateIndexes(model, options, callback);
        });
      } else {
        self._updateIndexes(model, options, callback);
      }
    });
    this.redis[cmd.fn].apply(this.redis, cmd.args);
  },

  inc: function (model, options, callback) {
    var key = this.getIdKey(model, options);
    debug('inc: ' + key);
    var incFn = this._getIncFn(model, options);
    var attribute = options.inc.attribute;
    var amount = options.inc.amount || 1;
    this.redis[incFn](key, attribute, amount, callback);
  },

  destroy: function(model, options, callback) {
    // force wait option, since otherwise Backbone removes Model's reference to collection
    // which is required for clearing indexes
    options.wait = true;
    var self = this;
    var key = this.getIdKey(model, options);
    debug("DESTROY: " + key);
    if (model.isNew()) {
      return false;
    }

    function delKey() {
      debug('removing key: ' + key);
      self.redis.del(key, function(err, res) {
        callback(err, model.toJSON());
      });
    }

    this._updateIndexes(model, _.extend({
      operation: 'delete'
    }, options), function(err) {
      if (model.collection) {
        var setKey = self.getIdKey(model.collection, {});
        var modelKey = model.get(model.idAttribute);
        debug('removing model ' + modelKey + ' from ' + setKey);
        self.redis.srem(setKey, modelKey, function(err, res) {
          if (err) return callback(err);
          delKey();
        });
      } else {
        debug('model has no collection specified');
        delKey();
      }
    });
  },

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

    var getReadFn = function() {
      var params;
      if (collection.indexSort) {
        var min = '-inf';
        var max = '+inf';
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
        } else {
          var start = options.offset || 0;
          var stop = options.limit ? (start + options.limit - 1) : -1;
          if (options.sortOrder === 1) {
            return _.bind(self.redis.zrange, self.redis, setKey, start, stop);
          }
          return _.bind(self.redis.zrevrange, self.redis, setKey, start, stop);
        }
      }
      if (options.sort && collection.model.prototype.redis_type === 'hash') {
        dynamicSorted = true;
        var m = new collection.model();
        var idKey = self.getIdKey(m);
        var parsedSort = utils.parseSort(options.sort);
        var sortParams = idKey + ':*->' + parsedSort.sortProp;
        params = [setKey, 'BY', sortParams];

        if (options.limit_query !== false && (options.limit || options.offset)) {
          params = params.concat(['LIMIT', options.offset || 0, options.limit || -1]);
        }

        if (parsedSort.sortOrder === -1) {
          params.push('DESC');
        }
        debug('dynamic sort:', params);
        return _.bind.apply(null, [self.redis.sort, self.redis].concat(params));
      }

      return _.bind(self.redis.smembers, self.redis, setKey);
    };

    // TODO: handle sort order
    var readWithRank = function(_done) {
      debug('readWithRank');
      if (!collection.indexSort) throw new Error('Cannot read rank of non-sorted set');
      var id = options.before_id ? options.before_id : options.after_id;
      // by default order set descending
      var order = options.sortOrder ? options.sortOrder : - 1;
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
        //debug('got rank: %s for id: %s, using %s %s', rank, id, rankFn, rangeFn);
        if (options.after_id) {
          start = rank + 1;
          stop = options.limit ? (start + options.limit -1) : - 1;

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
          if (options.limit) stop = start -1 + options.limit;
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

  /**
   * Read from multiple sets, storing union in new set temporarily
   */
  readFromIndexes: function(collection, options, cb) {
    var self = this;
    var unionKey = options.unionKey;
    var params = _.clone(options.indexKeys);
    if (collection.indexSort) params.unshift(options.indexKeys.length); // how many sets to union
    params.unshift(unionKey); // where to store
    if (options.weights) {
      params.push('WEIGHTS');
      params.push.apply(params, options.weights);
    }
    // by default use MAX aggregate option (redis default is SUM)
    if (collection.indexSort) {
      params.push('AGGREGATE', 'MAX');
    }

    var unionFn = collection.indexSort ?
      _.bind(this.redis.zunionstore, this.redis) :
      _.bind(this.redis.sunionstore, this.redis);
    unionFn(params, function(err) {
      self.redis.expire(unionKey, 300);
      options.indexKey = unionKey;
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

  indexCount: function(collection, options, cb) {
    var setKey = collection.indexKey;
    debug('get count for: ' + setKey);
    if (collection.indexSort) {
      this.redis.zcard(setKey, cb);
    } else {
      this.redis.scard(setKey, cb);
    }
  },

  findKeys: function(collection, options, cb) {
    var prefix = options.prefix || (this.getIdKey(collection, {}));
    if (prefix.indexOf(':', prefix.length - 1) === -1
      && options.keys.length) {
      prefix += ':';
    }
    this.redis.keys(prefix + options.keys + '*', cb);
  },

  _updateIndexes: function(model, options, callback) {
    if (!model.indexes) {
      debug('nothing to index');
      return callback(null, model.toJSON());
    }
    var operation = options.operation || 'add';
    var indexingOpts = {
      db: this,
      indexes: model.indexes,
      data: model.attributes,
      prevData: operation === 'delete' ? model.attributes : model.previousAttributes(),
      operation: operation,
      model: model,
      id: model.id
    };
    indexing.updateIndexes(indexingOpts, callback);
  }
});
RedisDb.sync = RedisDb.prototype.sync;
RedisDb.Hash = require('./lib/hash');
module.exports = RedisDb;
