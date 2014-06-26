var _ = require('lodash');
var debug = require('debug')('backbone-db-redis:query');
var utils = require('./utils');

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
    var self = this;
    this.requiredIndexes = [];
    if (this.filterOptions.customIndexes) {
      // custom indexes override default query logic
      this.requiredIndexes = this.filterOptions.customIndexes;
    } else {
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
    }


    if (this.ids) {
      // no need to query, since we know ids already
      return cb(null, null);
    }

    var query = _.clone(this.requiredIndexes);
    if (query.length > 1 || this.filterOptions.sort) {
      this.destinationSet = 'temp' + (Date.now() * Math.random());
      this.idQueryNr = 1;
      var sortOpts = this._getSortSetKeyAndOrder();
      var canUseIndexForSorting = false;
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
        this._customSort(this.destinationSet, this.filterOptions.customSort, cb);
      } else if (!canUseIndexForSorting && self.dbOptions.ModelClass.prototype.redis_type === 'hash') {
        this._sortUsingHash(this.destinationSet, {}, cb);
      } else {
        this._fetchSorted(this.destinationSet, cb);
      }

    } else {
      this.db.redis.smembers(query[0], cb);
    }

    debug('searching', this.filterOptions.where, 'from', this.requiredIndexes);
  },

  fetchAll: function(cb) {
    if (!this.filterOptions.sort) {
      var collectionKey = this.dbOptions.collectionKey;
      this.db.redis.sort(collectionKey, 'BY', 'nosort', cb);
    } else {
      this._fetchSorted(null, cb);
    }
  },

  searchByIds: function(ids, callback) {
    if (!ids.length) return callback(null, []);
    //TODO: already apply limit & offset on query
    ids = ids.splice(this.offset, this.limit);
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
    var multi = this.db.redis.multi();
    _.each(keys, function(key) {
      multi.hgetall(key);
    });
    multi.exec(function(err, results) {
      callback(err, results);
    });
  },

  _customSort: function(set, customSort, cb) {
    var parsedSort = utils.parseSort(this.filterOptions.sort);
    var sortParams = this.db.key(customSort[parsedSort.sortProp]);
    var params = [set, 'BY', sortParams];
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
    var params = [set, 'BY', sortParams];
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
