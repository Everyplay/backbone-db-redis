var assert = require('assert');
var _ = require('lodash' );
var Promises = require('backbone-promises');
var when = Promises.when;
var sequence = require('when/sequence');
var setup = require('./setup');
var MyCollection = setup.MyCollection;
var MyModel = setup.MyModel;
var store = setup.store;
var TestCollection = setup.TestIndexedCollection;

var TestCollection2 = TestCollection.extend({
  indexKey: 'test:i:Foo:relation2'
});

describe('Test IndexedCollection', function () {
  var collection;
  var collection2;

  before(function(done) {
    collection = new TestCollection();
    var fns = [
      collection.create({data: 'aaa', score: 22}),
      collection.create({data: 'bbb', score: 1}),
      collection.create({data: 'ccc', score: 99})
    ];
    when.all(fns).then(function() {
      done();
    }).otherwise(done);
  });

  after(function(done) {
    setup.clearDb(done);
  });

  it('should index a new item', function(done) {
    collection
      .addToIndex(collection.at(0))
      .then(function() {
        done();
      }).otherwise(done);
  });

  it('should index another item', function(done) {
    collection
      .addToIndex(collection.at(1))
      .then(function() {
        done();
      }).otherwise(done);
  });

  it('should index another item', function(done) {
    collection
      .addToIndex(collection.at(2))
      .then(function() {
        done();
      }).otherwise(done);
  });


  it('should get count from index', function(done) {
    collection = new TestCollection();
    collection
      .count()
      .then(function(count) {
        assert.equal(count, 3);
        done();
      }).otherwise(done);
  });

  it('should read ids with given score options', function(done) {
    var opts = {
      score: {
        min: 22,
        max: 999,
        // defines how to format scores into model
        conversion: {
          fn: function(score) {
            return score;
          },
          attribute: 'score'
        }
      },
      limit: 2,
      offset: 1
    };
    collection = new TestCollection();
    collection
      .readFromIndex(opts)
      .then(function() {
        assert.equal(collection.length, 1);
        assert.equal(collection.at(0).get('score'), '22');
        done();
      }).otherwise(done);
  });

  it('should read ids from index', function(done) {
    collection = new TestCollection();
    collection
      .readFromIndex()
      .then(function() {
        assert.equal(collection.length, 3);
        assert.equal(collection.pluck('id').length, 3);
        done();
      }).otherwise(done);
  });

  it('should fetch models', function(done) {
    var fetchOpts = {
      where: {
        id: {
          $in: collection.pluck('id')
        }
      }
    };
    collection
      .fetch(fetchOpts)
      .then(function() {
        assert.equal(collection.length, 3);
        assert.equal(collection.at(0).get('data'), 'ccc');
        assert.equal(collection.at(1).get('data'), 'aaa');
        done();
      }).otherwise(done);
  });

  it('should read with limit', function() {
    collection = new TestCollection();
    return collection
      .readFromIndex({limit: 1, offset: 0})
      .then(function() {
        collection.length.should.equal(1);
        collection.at(0).id.should.equal('3');
      });
  });

  it('should read with offset', function() {
    collection = new TestCollection();
    return collection
      .readFromIndex({limit: 2, offset: 1})
      .then(function() {
        collection.length.should.equal(2);
        collection.at(0).id.should.equal('1');
        collection.at(1).id.should.equal('2');
      });
  });


  it('should add item to another index', function(done) {
    collection2 = new  TestCollection2();
    collection2
      .create({data: 'ddd', score: 22})
      .then(function(m) {
        collection2
          .addToIndex(m)
          .then(function() {
            done();
          }).otherwise(done);
      }).otherwise(done);
  });

  it('should read from multiple indexes', function(done) {
    var opts = {
      indexKeys: ['test:i:Foo:relation', 'test:i:Foo:relation2'],
      unionKey: 'test:i:UnionFoo'
    };
    collection2 = new  TestCollection2();
    collection2
      .readFromIndexes(opts)
      .then(function() {
        assert.equal(collection2.length, 4);
        done();
      }).otherwise(done);
  });

  it('should read from multiple indexes with weights', function() {
    var opts = {
      indexKeys: ['test:i:Foo:relation'],
      unionKey: 'test:i:UnionFoo',
      weights: [1, 10],
      score: {
        conversion: {
          fn: function(score) {
            return score;
          },
          attribute: 'score'
        }
      }
    };
    collection2 = new  TestCollection2();
    return collection2
      .readFromIndexes(opts)
      .then(function() {
        assert.equal(collection2.length, 4);
        collection2.at(0).id.should.equal('4');
      });
  });

  it('should fetch models', function(done) {
    var fetchOpts = {
      where: {
        id: {
          $in: collection2.pluck('id')
        }
      }
    };
    collection2
      .fetch(fetchOpts)
      .then(function() {
        assert.equal(collection2.length, 4);
        assert(collection2.findWhere({data: 'ddd'}));
        done();
      }).otherwise(done);
  });

  it('should check that model exists in index', function(done) {
    var model = collection.at(0);
    assert(model);
    collection
      .exists(model)
      .then(function(exists) {
        assert.equal(exists, true);
        done();
      }).otherwise(done);
  });

  it('should fetch keys starting with given string', function(done) {
    collection
      .findKeys('')
      .then(function(keys) {
        assert(keys.length > 0);
        done();
      }).otherwise(done);
  });

  /*
  it('should remove model from index', function(done) {
    var model = collection.at(0);
    assert(model);
    collection
      .removeFromIndex(model)
      .then(function() {
        assert.equal(collection.length, 2);
        done();
      }).otherwise(done);
  });

  it('should check that model was removed from index', function(done) {
    collection = new TestCollection();
    collection
      .readFromIndex()
      .then(function() {
        assert.equal(collection.length, 2);
        assert(collection.at(0).get('data') !== 'ccc');
        done();
      }).otherwise(done);
  });

  it('should remove index', function(done) {
    collection = new TestCollection();
    var fns = [
      _.bind(collection.destroyAll, collection),
      _.bind(collection.readFromIndex, collection)
    ];
    sequence(fns)
      .then(function() {
        assert.equal(collection.length, 0);
        done();
      }).otherwise(done);
  });*/

});