var Deferred = require('backbone-promises');
var Backbone = require('backbone');
var debug = require('debug')('backbone-db-redis:hash');

var Hash = module.exports = Deferred.Model.extend({
  constructor: function() {
    Deferred.Model.apply(this, arguments);
  },
  hdel: function() {

  },
  hincrby: function(field, val, opt) {
    opt = opt || {};
    var self = this,
      db = this.db || this.collection.db;

    return this.defer('hincrby', opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hmget requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      var key = db.key(self.url());
      db.redis.hincrby(key, field, val, function(err, res) {
        cb(err, res);
      });
    });
  },
  hmget: function(fields, opt) {
    opt = opt || {};
    var self = this,
        db = this.db || this.collection.db;

    return this.defer('hmget', opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hmget requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      var key = db.key(self.url());
      db.redis.hmget(key, fields, function(err, res) {
        self.set(res);
        cb(err, res);
      });
    });
  },
  hvals: function() {

  },
  hexists: function() {

  },
  hincrbyfloat: function() {

  },
  hmset: function(opt) {
    opt = opt || {};
    var self = this,
        db = this.db || this.collection.db;

    return this.defer("hmset", opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hset requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      var obj = {};
      Object.keys(self.attributes).forEach(function(attr) {
        if(typeof self.attributes[attr] === "object") {
          obj[attr] = JSON.stringify(self.attributes[attr]);
        } else {
          obj[attr] = self.attributes[attr];
        }
      });
      db.redis.hmset(db.key(self.url()), obj, cb);
    });
  },
  hget: function(field, opt) {
    opt = opt || {};
    var self = this,
        db = this.db || this.collection.db;

    return this.defer('hget', opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hget requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      var key = db.key(self.url());
      db.redis.hget(key, field, function(err, res) {
        if(err) return cb(err);

        if(res) {
          self.set(field, res);
        }
        cb(null, res);
      });
    });
  },
  hkeys: function() {

  },
  hset: function(field, value, opt) {
    opt = opt || {};
    var self = this,
        db = this.db || this.collection.db;

    return this.defer('hset', opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hset requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      var key = db.key(self.url());
      db.redis.hset(key, field, value, function(err, res) {
        if(res) {
          debug('set %s = %s',field, value);
          self.set(field, value);
        }
        cb(err, value);
      });
    });
  },
  hgetall: function(opt) {
    opt = opt || {};
    var self = this,
        db = this.db || this.collection.db;
    return self.defer('hgetall', opt, function(cb) {
      if(!self.url || !db) {
        return cb(new Error("hgetall requires the collection to have an .url function and backbone-redis-db.RedisDb in .db"));
      }
      db.redis.hgetall(db.key(self.url()), function(err, res) {
        if(err) return cb(err);
        self.set(res);
        cb(null, self);
      })
    });
  },
  hlen: function() {

  },
  hsetnx: function() {

  },
  fetch: function(keys) {
    var self = this;
    return this.defer("fetch", function(cb) {
      if(keys) {
        self.hmget(keys, cb);
      } else {
        self.hgetall(cb);
      }
    });
  },
  save: function(opt) {
    opt = opt || {};
    var self = this;
    debug('saving hash %s', self.url());
    return this.defer("save", function(cb) {
      if(self.hasChanged()) {
        self.hmset(cb);
      } else {
        cb(null, self);
      }
    });
  }
})