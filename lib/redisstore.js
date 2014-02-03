var redis = require('redis')
  , debug = require('debug')('store')
  , _ = require('underscore');

/**
 * small redis string k=>v implementation
 */

function RedisStore(name, client) {
  this.name = name;
  this.client = client;
  if(!this.client) {
    this.client = redis.createClient();
  }
}

RedisStore.prototype.getItem = function(key, cb) {
  debug('getItem');
  this.client.get(this.name+key, cb);
};

RedisStore.prototype.setItem = function(key, value, cb) {
  debug('setItem');
  this.client.set(this.name+key, cb);
};

RedisStore.prototype.removeItem = function(key, cb) {
  debug('removeItem');
  this.client.del(this.name+key, cb);
};

RedisStore.prototype.getItems = function(key, start, end, cb) {
  debug('getItems');
  this.client.zrange(key,start,end, cb);
};


module.exports = RedisStore;