var assert = require('assert');
var setup = require('./setup');
var MyCollection = setup.MyCollection;
var MyModel = setup.MyModel;

describe('RedisDB#Collection', function() {
  var testCol = new MyCollection();
  var testModel;

  after(function(done) {
    setup.clearDb(function(err) {
      done(err);
    });
  });

  it('should create a model', function(done) {
    testCol
      .create({'id_check': 1}, { wait: true })
      .then(function(m) {
        assert(m.get('id_check') === testCol.at(0).get('id_check'));
        testModel = m;
        done();
      }).otherwise(done);
  });

  it('should fetch created model', function(done) {
    var m2 = new MyModel({id: testModel.id});
    m2.fetch().then(function(m) {
      assert(m.get('id_check') === m2.get('id_check'));
      done();
    }).otherwise(done);
  });

  it('should fetch collection models', function(done) {
    var collection = new MyCollection();
    collection
      .fetch()
      .then(function(c) {
        assert(collection.length === 1);
        assert(c.at(0));
        done();
      }).otherwise(done);
  });

  it('should remove model from collection', function(done) {
    var testId = testModel.id;
    testModel
      .destroy()
      .then(function() {
        var a = new MyCollection();
        a.fetch().then(function() {
          var removedModel = a.where({id: testId});
          assert(removedModel.length === 0);
          done();
      }).otherwise(done);
    }).otherwise(done);
  });

});
