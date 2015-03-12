var Promises = require('backbone-promises');

module.exports = Promises.Collection.extend({
  redis_type: 'set',
  model: Promises.Model
});
