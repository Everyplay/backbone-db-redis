var Promises = require('backbone-promises');
var debug = require('debug')('backbone-db-redis:set');
var _ = require('lodash');

module.exports = Promises.Collection.extend({
  redis_type: 'set',
  model: Promises.Model
});
