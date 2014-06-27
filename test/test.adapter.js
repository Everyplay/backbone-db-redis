require('chai').should();
var when = require('when');
var DB = require('../');
var Promises = require('backbone-promises');
var Collection = Promises.Collection;
var Model = Promises.Model;
var Hash = require('../lib/hash');
var SortedSet = require('../lib/sortedset');
var List = require('../lib/list');
var SetModel = require('../lib/set');
var setup = require('./setup');
var redis = setup.store.redis;
var async = require('async');
var _ = require('lodash');

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

var HashIndexCollection = setup.TestIndexedCollection.extend({
  model: HashModel,
  indexKey: 'test:i:hashindex',
  indexSort: null
});

var CustomIndexedHashCollection = HashCollection.extend({
  customIndex: 'i:custom',
  url: function() {
    return this.customIndex;
  }
});

describe('RedisDB redis adapter', function() {
  var db;
  var collection;
  var index;

  before(function() {
    db = new DB('tests');
    HashModel.prototype.sync = db.sync;
    HashModel.prototype.db = db;
    HashCollection.prototype.sync = db.sync;
    HashCollection.prototype.db = db;
    HashIndexCollection.prototype.sync = db.sync;
    HashIndexCollection.prototype.db = db;
    HashIndexCollection.prototype.indexDb = db;
    index = new HashIndexCollection();
  });

  after(function(next) {
    var fns = [
      collection.at(0).destroy(),
      collection.at(1).destroy(),
      collection.at(2).destroy(),
      index.destroyAll(),
    ];
    return Promises.when.all(fns).then(function() {
      setup.clearDb(next);
    });
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
      d: false,
      e: new Date(),
      f: ['aa'],
      g: 'ddd'
    };
    var data2 = {
      a: 225,
      b: Math.random(),
      c: 'abc',
      d: true,
      g: 'aaa'
    };
    var data3 = {
      a: 125,
      b: Math.random(),
      c: 'gde',
      d: false,
      g: 'aaa'
    };

    collection = new HashCollection();
    var fns = [
      collection.create(data),
      collection.create(data2),
      collection.create(data3)
    ];
    return Promises.when
      .all(fns)
      .then(function() {
        collection.length.should.equal(3);
      });
  });

  it('should fetch hash collection models', function() {
    var coll = new HashCollection();
    return coll
      .fetch({where: {c: 'abc'}})
      .then(function() {
        coll.length.should.equal(2);
        var model = coll.at(0);
        model.get('c').should.equal('abc');
        model.get('f').should.contain('aa');
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

  it('should inc Hash model property', function() {
    var coll = new HashCollection();
    return coll
      .fetch({where: {d: true}})
      .then(function() {
        var h = coll.at(0);
        h.get('a').should.equal(225);
        var saveOpts = {
          inc: {
            attribute: 'a',
            amount: 1
          }
        };
        return h.save(null, saveOpts).then(function() {
          return h.fetch();
        }).then(function(){
          h.get('a').should.equal(226);
        });
      });
  });

  it('should add models to index', function() {
    var coll = new HashCollection();
    return coll
      .fetch()
      .then(function() {
        return when.all([
          index.addToIndex(coll.at(0)),
          index.addToIndex(coll.at(1)),
          index.addToIndex(coll.at(2))
        ]);
      });
  });

  it('should read from index sorted by `a`', function() {
    return index
      .readFromIndex({sort: 'a'})
      .then(function() {
        index.length.should.equal(3);
        index.at(0).get('a').should.equal(124);
      });
  });

  it('should read from index sorted by `-a`', function() {
    return index
      .readFromIndex({sort: '-a'})
      .then(function() {
        index.length.should.equal(3);
        index.at(0).get('a').should.equal(226);
      });
  });

  it('should add models to customIndex', function(next) {
    var coll = new HashCollection();
    coll
      .fetch()
      .then(function() {
        var fns = [];
        collection.each(function(model) {
          fns.push(_.bind(redis.sadd, redis, 'tests:i:custom', model.id));
          fns.push(_.bind(redis.set, redis, 'tests:customsort:' + model.id, 500 - model.get('a')));
        });
        async.parallel(fns, function(err, res) {
          next();
        });
      });
  });

  it('should fetch from customIndex', function() {
    var coll = new CustomIndexedHashCollection();
    return coll
      .fetch()
      .then(function() {
        coll.length.should.equal(3);
      });
  });

  it('should fetch from customIndex sorting by value set to customsort', function() {
    var opts = {
      sort: 'a',
      customSort: {
        a: 'customsort:*'
      }
    };
    var coll = new CustomIndexedHashCollection();
    return coll
      .fetch(opts)
      .then(function() {
        coll.length.should.equal(3);
        coll.at(0).get('a').should.equal(226);
      });
  });

  it('should defer to customIndex with where params', function() {
    var opts = {
      where: {
        g: 'aaa'
      },
      sort: '-a',
      customSort: {
        a: 'customsort:*'
      }
    };
    var coll = new CustomIndexedHashCollection();
    opts.customIndex = 'tests:' + coll.url();
    return coll
      .fetch(opts)
      .then(function() {
        coll.length.should.equal(3);
        coll.at(0).get('a').should.equal(124);
    });
  });
});