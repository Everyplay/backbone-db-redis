require('mocha-as-promised')();
require('chai').should();
var DB = require('../');
var Promises = require('backbone-promises');
var Collection = Promises.Collection;
var Model = Promises.Model;
var Hash = require('../lib/hash');
var SortedSet = require('../lib/sortedset');
var List = require('../lib/list');
var SetModel = require('../lib/set');

var HashModel = Hash.extend({
  type: 'hashmodel',
  dbBaseKey: 'hashmodels',
  url: function() {
    var key = this.dbBaseKey || this.type;
    if (!this.isNew()) {
      key += ':' + this.get(this.idAttribute);
    }
    return key;
  },
  indexes: [
    {property: 'c', sort: 'asc', unique: true},
    {property: 'd'}
  ]
});

var HashCollection = Collection.extend({
  model: HashModel,
  type: 'hashmodels',
  url: function() {
    return this.type;
  }

});

describe('RedisDB redis adapter', function() {
  var db;
  var collection;

  before(function() {
    db = new DB('tests');
    HashModel.prototype.sync = db.sync;
    HashModel.prototype.db = db;
    HashCollection.prototype.sync = db.sync;
    HashCollection.prototype.db = db;
  });

  after(function() {
    var fns = [
      collection.at(0).destroy(),
      collection.at(1).destroy(),
    ];
    return Promises.when.all(fns);
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

  it('should create hash models using collection', function() {
    var data = {
      a: 124,
      b: Math.random(),
      c: 'abc',
      d: false
    };
    var data2 = {
      a: 125,
      b: Math.random(),
      c: 'abc',
      d: true
    };
    collection = new HashCollection();
    var fns = [
      collection.create(data),
      collection.create(data2),
    ];
    return Promises.when
      .all(fns)
      .then(function() {
        collection.length.should.equal(2);
      });
  });

  it('should fetch hash collection models', function() {
    var coll = new HashCollection();
    return coll
      .fetch({where: {c: 'abc'}})
      .then(function() {
        coll.length.should.equal(2);
      });
  });

  it('should fetch models with d:true', function() {
    var coll = new HashCollection();
    return coll
      .fetch({where: {d: true}})
      .then(function() {
        coll.length.should.equal(1);
      });
  });
});