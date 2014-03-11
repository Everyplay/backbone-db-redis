var Promises = require('backbone-promises');

var SortedSet = module.exports = Promises.Collection.extend({
  redis_type: 'zset',
  model: Promises.Model
});