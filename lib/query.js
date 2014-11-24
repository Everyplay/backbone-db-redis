var _ = require('lodash');
var debug = require('debug')('backbone-db-redis:query');
var utils = require('./utils');
var async = require('async');

function objToJSON(data) {
  var obj = {};
  var keys = _.keys(data);
  var len = keys.length;
  for (var i = 0; i < len; ++i) {
    try {
      obj[keys[i]] = JSON.parse(data[keys[i]]);
    } catch (e) {
      obj[keys[i]] = data[keys[i]];
    }
  }
  return obj;
}

function toJSON(res) {
  if (res) {
    res = res.map(function(data) {
      if (_.isObject(data)) {
        return objToJSON(data);
      } else {
        return data && JSON.parse(data);
      }
    });
  }
  if (_.isArray(res) && _.every(res, function(m) {
    return !m;
  }))  {
    return [];
  }
  return res;
}

var DbQuery = function(dbOptions, filterOptions) {
  this.dbOptions = dbOptions;
  this.filterOptions = filterOptions;
  this.db = dbOptions.db;
  this.model = dbOptions.model;
  this.redis_type = dbOptions.redis_type;
  this.ModelClass = dbOptions.ModelClass;
  this.limit = this.filterOptions.limit ? this.filterOptions.limit : 50;
  this.offset = this.filterOptions.offset ? this.filterOptions.offset : 0;
};

_.extend(DbQuery.prototype, {
  execute: function(callback) {
    var self = this;
    var fetchFn;
    if (this.filterOptions.where) {
      fetchFn = this.filterModels.bind(this);
    } else {
      fetchFn = this.fetchAll.bind(this);
    }

    this._offsetApplied = false;

    fetchFn(function(err, results) {
      debug('results:', results);
      if (self.destinationSet) self.db.redis.del(self.destinationSet);
      if (self.ids) {
        return self.searchByIds(self.ids, callback);
      }

      var resultError = _.find(results, function(res) {
        return res.toString().indexOf('ERR') > -1;
      });
      if (err || !results || resultError) {
        return callback(err || resultError, []);
      }
      var ids = results;

      self.searchByIds(ids, callback);
    });
  },

  filterModels: function(cb) {
    // if custom index is used don't try to make dynamic filtering
    if (this.filterOptions.customIndex) return this.fetchAll(cb);
    var self = this;
    this.requiredIndexes = [];

    // custom indexes override default query logic
    _.each(this.filterOptions.where, function(val, key) {
      if (_.isObject(val) && val.hasOwnProperty('$in')) {
        val = val.$in;
      }
      if (key === 'id') {
        // searching ids is a special case,
        // no other filters supported
        debug('search by id', val);
        this.ids = val;
      } else {
        var dbKey = this.db.getValueSetKey(this.dbOptions.model, key, val);
        this.requiredIndexes.push(dbKey);
      }
    }, this);


    if (this.ids) {
      // no need to query, since we know ids already
      return cb(null, null);
    }

    var query = _.clone(this.requiredIndexes);
    if (query.length > 1 || this.filterOptions.sort) {
      var canUseIndexForSorting = false;
      this.destinationSet = 'temp' + (Date.now() * Math.random());
      this.idQueryNr = 1;
      var sortOpts = this._getSortSetKeyAndOrder();
      // if sorting, check if we can use a set for sorting, otherwise try
      // to sort dynamically
      if (sortOpts.sortKey) {
        _.each(self.dbOptions.indexes, function(index) {
          var indexSet = self.db.getSortSetKey(self.model, index.property);
          if (indexSet === sortOpts.sortKey) canUseIndexForSorting = true;
        });

        if (canUseIndexForSorting) {
          query.push(sortOpts.sortKey);
        } else {
          debug('no available index found for %s, try dynamic sort', sortOpts.sortKey);
        }
      }
      query.unshift(query.length);
      query.unshift(this.destinationSet);
      this.db.redis.zinterstore.apply(this.db.redis, query);

      if (this.filterOptions.customSort) {
        return this._customSort(this.destinationSet, this.filterOptions.customSort, cb);
      } else if (!canUseIndexForSorting && self.dbOptions.ModelClass.prototype.redis_type === 'hash') {
        this._sortUsingHash(this.destinationSet, {}, cb);
      } else {
        this._fetchSorted(this.destinationSet, cb);
      }

    } else {
      // no sorting
      this.db.redis.smembers(query[0], cb);
    }

    debug('searching', this.filterOptions.where, 'from', this.requiredIndexes);
  },

  fetchAll: function(cb) {
    var collectionKey = this.dbOptions.collectionKey;
    if (!this.filterOptions.sort) {
      this._offsetApplied = true;
      this.db.redis.sort(collectionKey, 'BY', 'nosort', 'LIMIT', this.offset, this.limit, cb);
    } else {
      if (this.filterOptions.customSort) {
        return this._customSort(collectionKey, this.filterOptions.customSort, cb);
      }
      this._fetchSorted(null, cb);
    }
  },

  searchByIds: function(ids, callback) {
    if (!ids.length) return callback(null, []);

    if (this._offsetApplied !== true) {
      ids = ids.splice(this.offset, this.limit);
    }
    var keys = _.map(ids, function(id) {
      // if indexed value is the hash key
      if (_.isString(id) && id.indexOf('h:') > -1) {
        var parts = id.split(':');
        id = parts[parts.length -1];
      }
      var attrs = {};
      attrs[this.ModelClass.prototype.idAttribute] = id;
      var model = new this.ModelClass(attrs);
      return this.db.getIdKey(model);
    }, this);

    if (!keys.length) {
      debug('nothing found');
      return callback(null, []);
    }
    // read hashes or strings
    if (this.dbOptions.redis_type === 'hash') {
      this.mgetHashes(keys, function(err, res) {
        res = _.filter(res, function(r) {
          return r !== null;
        });
        callback(err, toJSON(res));
      });
    } else {
      this.db.redis.mget(keys, function(err, res) {
        callback(err, toJSON(res));
      });
    }
  },

  mgetHashes: function(keys, callback) {
    var redis = this.db.redis;
    async.map(keys, redis.hgetall.bind(redis), callback);
  },

  _customSort: function(set, customSort, cb) {
    var parsedSort = utils.parseSort(this.filterOptions.sort);
    var sortParams = this.db.key(customSort[parsedSort.sortProp]);
    this._offsetApplied = true;
    var params = [set, 'BY', sortParams, 'LIMIT', this.offset, this.limit];
    if (parsedSort.sortOrder === -1) {
      params.push('DESC');
    }
    debug('customSort', params);
    this.db.redis.sort(params, cb);
  },

  _sortUsingHash: function(set, options, cb) {
    var m = new this.dbOptions.ModelClass();
    var idKey = this.db.getIdKey(m);
    var parsedSort = utils.parseSort(this.filterOptions.sort);
    var sortParams = idKey + ':*->' + parsedSort.sortProp;
    this._offsetApplied = true;
    var params = [set, 'BY', sortParams, 'LIMIT', this.offset, this.limit];
    if (parsedSort.sortOrder === -1) {
      params.push('DESC');
    }
    this.db.redis.sort(params, cb);
  },

  _fetchSorted: function(set, cb) {
    var self = this;
    var sortOpts = this._getSortSetKeyAndOrder();
    set = set ? set : sortOpts.sortKey;

    var start = 0;
    var end = -1;
    debug('fetch sorted', sortOpts, set);
    /*if(this.filterOptions.after_id) {
      this.multi.zrank(set, this.filterOptions.after_id, function(err, rank) {
        start = rank;
      });
    }*/

    if (sortOpts.sortOrder === undefined || sortOpts.sortOrder === 1) {
      this.db.redis.zrange(set, start, end, cb);
    } else {
      this.db.redis.zrevrange(set, 0, -1, cb);
    }
  },

  _getSortSetKeyAndOrder: function() {
    if (!this.filterOptions.sort) return {};
    var parsedSort = utils.parseSort(this.filterOptions.sort);
    var sortKey = this.db.getSortSetKey(this.model, parsedSort.sortProp);
    return {
      sortKey: sortKey,
      sortOrder: parsedSort.sortOrder
    };
  }
});

exports.queryModels = function(filterOptions, dbOptions, callback) {
  var query = new DbQuery(dbOptions, filterOptions);
  query.execute(callback);
};
