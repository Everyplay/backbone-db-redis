var _ = require('lodash');
var Backbone = require('backdash');
var Db = require('backbone-db');
var redis = require('redis');
var debug = require('debug')('backbone-db-redis');
var indexing = require('./lib/indexing');
var query = require('./lib/query');
var indexedDbMixin = require('./lib/indexed_db_mixin');

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
  }
  return this.name + ':' + key;
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

_.extend(RedisDb.prototype, Db.prototype, indexedDbMixin, {
  createClient: function() {
    if (this.redis) {
      return redis.createClient(this.redis.port, this.redis.host);
    }
  },

  _getLoadFn: function(model) {
    var type = model.redis_type || 'string';
    return loadFnMap[type.toLowerCase()] || 'get';
  },
  _getSaveFn: function(model) {
    var type = model.redis_type || 'string';
    return saveFnMap[type.toLowerCase()] || 'set';
  },
  _getIncFn: function(model) {
    var type = model.redis_type || 'string';
    if (!incFnMap.hasOwnProperty(type)) {
      throw new Error('Cannot inc with type: ' + type);
    }
    return incFnMap[type.toLowerCase()];
  },
  _getSaveArgs: function(model, options, fn) {
    var args = [this.getIdKey(model, options)];
    options = options || {};
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
    options = options || {};
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
    }
    return key;
  },

  // get Redis key for set, where key/value is stored
  getValueSetKey: function(model, key, val) {
    var baseKey = model.dbBaseKey || model.type;
    var setKey = 'i:' + baseKey + ':' + key + ':' + val;
    if (this.name !== '') {
      return this.name + ':' + setKey;
    }

    return setKey;
  },

  // get Redis key for set for sorted property
  getSortSetKey: function(model, sortProp) {
    var baseKey = model.dbBaseKey || model.type;
    var setKey = 'i:' + baseKey + ':' + sortProp;
    if (this.name !== '') {
      return this.name + ':' + setKey;
    }
    return setKey;
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
      _.each(objectKeys, function(attr) {
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
      query.queryModels(options, dbOpts, function(queryErr, results) {
        callback(queryErr, results && results.length && results[0]);
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
    cmd.args.push(function(err) {
      if (err) return callback(err);
      if (model.collection) {
        var setKey = self.getIdKey(model.collection, {});
        var modelKey = model.get(model.idAttribute);
        debug('adding model ' + modelKey + ' to ' + setKey);
        self.redis.sadd(setKey, modelKey, function(addErr) {
          if (addErr) return callback(addErr);
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
    debug('DESTROY: ' + key);
    if (model.isNew()) {
      return false;
    }

    function delKey() {
      debug('removing key: ' + key);
      self.redis.del(key, function(err) {
        callback(err, model.toJSON());
      });
    }

    this._updateIndexes(model, _.extend({
      operation: 'delete'
    }, options), function(err) {
      if (err) return callback(err);
      if (model.collection) {
        var setKey = self.getIdKey(model.collection, {});
        var modelKey = model.get(model.idAttribute);
        debug('removing model ' + modelKey + ' from ' + setKey);
        self.redis.srem(setKey, modelKey, function(remErr) {
          if (remErr) return callback(remErr);
          delKey();
        });
      } else {
        debug('model has no collection specified');
        delKey();
      }
    });
  },

  // Warning: consider KEYS as a command that should only be used in
  // production environments with extreme care
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
