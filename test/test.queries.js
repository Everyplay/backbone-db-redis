var assert = require('assert');
var _ = require('underscore');
var Promises = require('backbone-promises');

var setup = require('./setup');
var redis = setup.store.redis;

var collection = new setup.IndexedCollection();

var inAscendingOrder = function(arr) {
  var inOrder = _.every(arr, function(value, index) {
    return index === 0 || arr[index - 1] <= value;
  });
  return inOrder;
};

var inDescendingOrder = function(arr) {
  var inOrder = _.every(arr, function(value, index) {
    return index === 0 || arr[index - 1] >= value;
  });
  return inOrder;
};

describe('Query tests', function() {
  var testModel;

  before(function(done) {
    setup.insertFixtureData(collection, done);
  });

  after(function(done) {
    setup.clearDb(function(err) {
      done(err);
    });
  });

  it('should fetch matching models filtered with where operator', function(done) {
    var opts = {
      where: {
        value: 2
      }
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 2);
        var allHaveCorrectValue = collection.all(function(model) {
          return model.get('value') === 2;
        });
        assert(allHaveCorrectValue);
        done();
      }).otherwise(done);
  });

  it('should filter with multiple where options', function(done) {
    var opts = {
      where: {
        value: 2,
        name: 'b'
      }
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 1);
        var allHaveCorrectValue = collection.all(function(model) {
          return model.get('value') === 2 && model.get('name') === 'b';
        });
        assert(allHaveCorrectValue);
        done();
      }).otherwise(done);
  });

  it('should fetch models with limit & offset', function(done) {
    var opts = {
      limit: 2,
      offset: 1,
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 2);
        var m = collection.at(0);
        assert(m.get('id') === 2);
        done();
      }).otherwise(done);
  });

  it('should fetch models sorted with value in ascending order', function(done) {
    var opts = {
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 4);
        var values = collection.pluck('value');
        assert(inAscendingOrder(values));
        done();
      }).otherwise(done);
  });

  it('should fetch models sorted with value in descending order', function(done) {
    var opts = {
      sort: '-value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 4);
        var values = collection.pluck('value');
        assert(inDescendingOrder(values));
        done();
      }).otherwise(done);
  });

  it('should fetch models with after_id', function(done) {
    //TODO
    var opts = {
      after_id: 2,
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        done();
      }).otherwise(done);
  });

  it('should fetch models with combined options #1', function(done) {
    var opts = {
      where: {name: 'c'},
      limit: 2,
      offset: 0,
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 2);
        var values = collection.pluck('value');
        assert(inAscendingOrder(values));
        done();
      }).otherwise(done);
  });

  it('should fetch models with combined options #2', function(done) {
    var opts = {
      where: {name: 'c'},
      limit: 2,
      offset: 0,
      sort: '-value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 2);
        var values = collection.pluck('value');
        assert(inDescendingOrder(values));
        done();
      }).otherwise(done);
  });

  it('should fetch models with combined options #3', function(done) {
    var opts = {
      where: {name: 'c'},
      limit: 2,
      offset: 1,
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 1);
        var values = collection.pluck('value');
        assert(values[0] === 3);
        done();
      }).otherwise(done);
  });

  it('should fetch models with combined options #4', function(done) {
    var opts = {
      where: {name: 'c', value: 2},
      limit: 2,
      offset: 0,
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 1);
        var values = collection.pluck('value');
        assert(values[0] === 2);
        done();
      }).otherwise(done);
  });

  it('should query models with $in', function(done) {
    var opts = {
      where: {
        platforms: {
          $in: 'android'
        }
      }
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 2, 'query should return 2 models');
        var ids = collection.pluck('id');
        assert(ids.indexOf(1) > -1);
        assert(ids.indexOf(2) === -1);
        assert(ids.indexOf(3) > -1);
        done();
      }).otherwise(done);
  });

  it('should query models with $in when combined with other filters', function(done) {
    var opts = {
      where: {
        platforms: {
          $in: 'android'
        },
        name: 'c'
      },
      sort: 'value'
    };
    collection
      .fetch(opts)
      .then(function() {
        assert(collection.length === 1, 'query should return 1 model');
        assert.equal(collection.at(0).get('name'), 'c');
        done();
      }).otherwise(done);
  });


});
