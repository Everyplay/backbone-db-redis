require('mocha-as-promised')();
require('chai').should();
var DB = require('../');
var Model = require('backbone-promises').Model;
var Hash = require('../lib/hash');
var SortedSet = require('../lib/sortedset');
var List = require('../lib/list');
var SetModel = require('../lib/set');

var HashModel = Hash.extend({
  url: function() {
    var key = this.dbBaseKey || this.type;
    if (!this.isNew()) {
      key += ':' + this.get(this.idAttribute);
    }
    return key;
  }
});

describe('RedisDB redis adapter', function() {
  var db;
  before(function() {
    db = new DB('tests');
    HashModel.prototype.sync = db.sync;
    HashModel.prototype.db = db;
  });

  it('should determine load and save function from models redis_type', function() {
    var s = new Model();
    var h = new HashModel();
    db._getLoadFn(s).should.equal('get');
    db._getLoadFn(h).should.equal('hgetall');
    db._getSaveFn(s).should.equal('set');
    db._getSaveFn(h).should.equal('hmset');
  });

  it('should correctly save hash models', function() {
    var h = new HashModel();
    h.set({a: 123, b: Math.random(), c: 'abc', d: false});
    return h.save().then(function() {
      var h2 = new HashModel({id: h.id});
      return h2.fetch().then(function() {
        h2.toJSON().should.deep.equal(h.toJSON());
        return h2.destroy();
      });
    });
  });
});