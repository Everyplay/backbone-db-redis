var Deferred = require('backbone-promises');
var Backbone = require('backbone');
var debug = require('debug')('backbone-db-redis:set');
var _ = require('underscore');

var Set = module.exports = Deferred.Collection.extend({
  constructor: function() {
    Deferred.Collection.apply(this, arguments);
  },
  sadd: function(values, options) {
    var self = this,
        db = this.db || this.model.db;
    options || (options = {});
    if(values.pluck) {
      values = values.pluck('id');
    } else if(!Array.isArray(values)) {
      values = [values];
    }
    return this.defer('sadd', options, function(cb) {
      if(!self.url) {
        return cb(new Error("url function is required for source and destination collections"));
      }
      var key = db.key(self.url());
      debug('Adding %s to redis set %s', JSON.stringify(values), key);
      db.redis.sadd(key, values, function(err, res) {
        return cb(err, res);
      });
    });
  },
  sinter: function(collection, opt) {
    var self = this,
      db = this.db || this.model.db;

    if(!self.url || !collection.url) {
      return cb(new Error("url function is required for source and destination collections"));
    }
    opt = opt || {};

    return this.defer('sinter', opt, function(cb) {
      var src = db.key(self.url());
      var dst = db.key(collection.url());
      debug('redis sunion %s %s',src, dst);
      db.redis.sinter(src, dst, function(err, ids) {
        if(err || (!ids || ids.length == 0)) {
          return cb(err, ids);
        }
        var done = _.after(ids.length, cb.success);
        ids.forEach(function(id) {
          var model = new self.model({id:id})
          self.add(model);
          debug('fetching %s',id);
          model.fetch().done(done).fail(cb);
        });
      });
    });
  },
  smove: function(collection, member, opt) {
    var self = this,
        db = this.db || this.model.db;
        opt = opt || {};

    if(!self.url || !collection.url) {
      return cb(new Error("url function is required for source and destination collections"));
    }

    return this.defer('smove', function(cb) {
      var src = db.key(self.url());
      var dst = db.key(collection.url());
      debug('redis smove %s %s',src, dst);
      if(!member) {
        member = self.at(0);
      }
      db.redis.smove(src, dst, member.get('id'), function(err, ids) {
        self.remove(member);
        collection.add(member);
        cb(err, self);
      });
    });
  },
  sunion: function(collection, opt) {
    var self = this,
        db = this.db || this.model.db;
    opt = opt || {};

    if(!self.url || !collection.url) {
      return cb(new Error("url function is required for source and destination collections"));
    }

    return this.defer('sunion', opt, function(cb) {
      var src = db.key(self.url());
      var dst = db.key(collection.url());
      debug('redis sunion %s %s',src, dst);
      db.redis.sunion(src, dst, function(err, ids) {
        if(err || (!ids || ids.length == 0)) {
          return cb(err, ids);
        }
        var done = _.after(ids.length, cb.success);
        ids.forEach(function(id) {
          var model = new self.model({id:id})
          self.add(model);
          debug('fetching %s',id);
          model.fetch().done(done).fail(cb);
        });
      });
    });
  },
  scard: function(opt) {
    var self = this,
        db = this.db || this.model.db;
    var src = db.key(self.url());
    opt = opt || {};

    return this.defer('scard', opt, function(cb) {
      debug('redis scard %s',src);
      db.redis.scard(src, cb);
    });
  },
  sinterstore: function() {

  },
  spop: function() {

  },
  sunionstore: function() {

  },
  sdiff: function() {
    var self = this,
      db = this.db || this.model.db;

    if(!self.url || !collection.url) {
      return cb(new Error("url function is required for source and destination collections"));
    }

    return this.defer('sdiff', function(cb) {
      var src = db.key(self.url());
      var dst = db.key(collection.url());
      debug('redis sdiff %s %s',src, dst);
      db.redis.sdiff(src, dst, function(err, ids) {
        if(err || (!ids || ids.length == 0)) {
          return cb(err, ids);
        }
        var done = _.after(ids.length, cb.success);
        ids.forEach(function(id) {
          var model = new self.model({id:id})
          self.add(model);
          debug('fetching %s',id);
          model.fetch().done(done).fail(cb);
        });
      });
    });
  },
  sismember: function() {

  },
  srandmember: function() {

  },
  sdiffstore: function() {

  },
  smembers: function() {

  },
  srem: function(ids, opt) {
    var self = this,
      db = this.db || this.model.db;
    opt || (opt = {});

    return this.defer('srem', opt, function(cb) {
      if(!self.url) {
        return cb(new Error("url function is required for source and destination collections"));
      }
      var key = db.key(self.url());
      debug('Removing %s from redis set %s', JSON.stringify(ids), key);
      db.redis.srem(key, ids, function(err, res) {
        return cb(err, res);
      });
    });
  },
  save: function(opt) {
    opt = opt || {};
    var self = this;
    var db = this.db || this.model.db;
    debug('saving set %s',this.url());
    return this.defer("save", opt, function(cb) {
      if(!db || !self.url) {
        return cb(new Error('.url or .db missing from set/set.model'));
      }
      debug('saving %s to %s', JSON.stringify(self.pluck(opt.idAttribute || 'id')), db.key(self.url()));
      self.sadd(self.pluck('id'), function(err, res) {
        return cb(err, res);
      });
    });
  },
  remove: function(models, opt) {
    var self = this;
    opt = opt || {};
    if(models && models.models) {
      models = models.models;
    }
    if(!Array.isArray(models)) {
      models = [models];
    }
    var ids = models.map(function(model) {
      return model.id || model.get(model.idAttribute);
    });
    return self.defer('remove', opt, function(cb) {
      self.srem(ids, function(err, res) {
        Deferred.Collection.prototype.remove.call(self, models);
        cb(err, res);
      });
    });
  }
});