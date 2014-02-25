var _ = require('underscore');
var debug = require('debug')('backbone-db-redis:query');

function toJSON(res) {
  if (res) {
    res = res.map(function(data) {
      return data && JSON.parse(data);
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
  this.ModelClass = dbOptions.ModelClass;
  this.limit = this.filterOptions.limit ? this.filterOptions.limit : 50;
  this.offset = this.filterOptions.offset ? this.filterOptions.offset : 0;
};

_.extend(DbQuery.prototype, {
  execute: function(callback) {
    var self = this;
    this.multi = this.db.redis.multi();
    if (this.filterOptions.where) {
      this.filterModels();
    } else {
      this.fetchAll();
    }

    if (this.destinationSet) this.multi.del(this.destinationSet);
    if (this.ids) {
      return this.searchByIds(this.ids, callback);
    }
    this.multi.exec(function(err, results) {
      var ids = results[self.idQueryNr];
      self.searchByIds(ids, callback);
    });
  },

  filterModels: function() {
    var self = this;
    this.requiredIndexes = [];
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
      return;
    }

    var query = _.clone(this.requiredIndexes);
    this.destinationSet = 'temp' + (Date.now() * Math.random());

    if (query.length > 1 || this.filterOptions.sort) {
      this.idQueryNr = 1;
      var sortOpts = this._getSortSetKeyAndOrder();
      if (sortOpts.sortKey) query.push(sortOpts.sortKey);
      query.unshift(query.length);
      query.unshift(this.destinationSet);
      this.multi.zinterstore(query);
      this._fetchSorted(this.destinationSet);
    } else {
      this.idQueryNr = 0;
      this.multi.smembers(query[0]);
    }

    debug('searching', this.filterOptions.where, 'from', this.requiredIndexes);
  },

  fetchAll: function() {
    if (!this.idQueryNr) this.idQueryNr = 0;
    if (!this.filterOptions.sort) {
      var collectionKey = this.dbOptions.collectionKey;
      this.multi.sort(collectionKey, 'BY', 'nosort');
    } else {
      this._fetchSorted();
    }
  },

  searchByIds: function(ids, callback) {
    if (!ids.length) return callback(null, []);
    //TODO: already apply limit & offset on query
    ids = ids.splice(this.offset, this.limit);
    var keys = _.map(ids, function(id) {
      var attrs = {};
      attrs[this.ModelClass.prototype.idAttribute] = id;
      var model = new this.ModelClass(attrs);
      return this.db.getIdKey(model);
    }, this);
    if (!keys.length) {
      debug('nothing found');
      return callback(null, []);
    }
    this.db.redis.mget(keys, function(err, res) {
      callback(err, toJSON(res));
    });
  },

  _fetchSorted: function(set) {
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
      this.multi.zrange(set, start, end);
    } else this.multi.zrevrange(set, 0, -1);
  },

  _getSortSetKeyAndOrder: function() {
    if (!this.filterOptions.sort) return {};
    var sortProp = this.filterOptions.sort;
    var sortOrder = 1;
    if (sortProp && sortProp[0] === "-") {
      sortOrder = -1;
      sortProp = sortProp.substr(1);
    }
    var sortKey = this.db.getSortSetKey(this.model, sortProp);
    return {
      sortKey: sortKey,
      sortOrder: sortOrder
    };
  }
});

exports.queryModels = function(filterOptions, dbOptions, callback) {
  var query = new DbQuery(dbOptions, filterOptions);
  query.execute(callback);
};