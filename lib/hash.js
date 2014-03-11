var Promises = require('backbone-promises');
var Backbone = require('backbone');
var debug = require('debug')('backbone-db-redis:hash');

var Hash = module.exports = Promises.Model.extend({
  redis_type: 'hash'
});
