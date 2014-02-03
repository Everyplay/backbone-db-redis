var Deferred = require('backbone-promises');
var Backbone = require('backbone');

var SortedSet = module.exports = Deferred.Collection.extend({
  model: Deferred.Model,
  constructor: function() {
    Deferred.Collection.apply(this, arguments);
    if(this.db) {
      this.redis = this.db.redis;
    } else {
      this.redis = this.model.db.redis;
    }
  },
  zadd: function(value) {

  },
  zinterstore: function() {

  },
  zrem: function() {

  },
  zrevrangebyscore: function() {

  },
  zcard: function() {

  },
  zrange: function() {

  },
  zremrangebyrank: function() {

  },
  zrevrank: function() {

  },
  zcount: function() {

  },
  zrangebyscore: function() {

  },
  zscore: function() {

  },
  zincrby: function() {

  },
  zrank: function() {

  },
  zrevrange: function() {

  },
  zunionstore: function() {

  }
});