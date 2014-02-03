var Deferred = require('backbone-promises');
var Backbone = require('backbone');

var List = module.exports = Deferred.Collection.extend({
  constructor: function() {
    Deferred.Collection.apply(this, arguments);
  },
  blpop: function(value) {
    var self = this;
    return this.defer("blpop", function(cb) {
      this.db.redis.blpop(this.url(), function(err, res) {
        if(err) return cb(err);
        var a = new self.model({id:res});
        a.fetch();
        this.add(a);
      });
    });
  },
  llen: function() {

  },
  rpush: function() {

  },
  brpop: function() {

  },
  lpop: function() {

  },
  lset: function() {

  },
  rpushx: function() {

  },
  brpoplpush: function() {

  },
  lpush: function() {

  },
  ltrim: function() {

  },
  lindex: function() {

  },
  lpushx: function() {

  },
  rpop: function() {

  },
  linsert: function() {

  },
  lrange: function() {

  },
  rpoplpush: function() {

  }
})