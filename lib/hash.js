var Promises = require('backbone-promises');

module.exports = Promises.Model.extend({
  redis_type: 'hash'
});
