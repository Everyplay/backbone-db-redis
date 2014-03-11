var Promises = require('backbone-promises');
var Backbone = require('backbone');

var List = module.exports = Promises.Collection.extend({
  redis_type: 'list'
});
