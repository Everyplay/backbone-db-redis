var assert = require('assert');

var setup = require('./setup');
var IndexedModel = setup.IndexedModel;
var redis = setup.store.redis;

var collection = new setup.IndexedCollection();

describe('Indexing tests', function() {

  before(function(done) {
    setup.insertFixtureData(collection, done);
  });

  after(function(done) {
    setup.clearDb(function(err) {
      done(err);
    });
  });

  it('should check that specified indexes were created', function(done) {
    function checkPlatforms() {
      redis.smembers('test:i:mymodels:platforms:ios', function(err, ids) {
        assert(ids.length === 3);
        assert(ids.indexOf('1') > -1);
        assert(ids.indexOf('2') > -1);
        assert(ids.indexOf('4') > -1);
        done();
      });
    }
    redis.keys('test:i:mymodel*', function(err, keys) {
      if (err) return done(err);
      assert(keys.indexOf('test:i:mymodels:value:1') > -1);
      assert(keys.indexOf('test:i:mymodels:platforms:android') > -1);
      redis.smembers('test:i:mymodels:value:2', function(err2, ids) {
        if (err2) return done(err2);
        assert(ids.indexOf('2') > -1);
        assert(ids.indexOf('4') > -1);
        checkPlatforms();
      });
    });
  });

  it('should fetch model with non-id attributes', function(done) {
    var model = new IndexedModel({value: 1});
    model
      .fetch()
      .then(function() {
        assert.equal(model.id, 1);
        assert.equal(model.get('value'), 1);
        done();
      }, done);
  });

  it('should result an error if trying to fetch model with non-unique attributes', function(done) {
    var model = new IndexedModel({name: 'a'});
    model
      .fetch()
      .then(function() {
        done(new Error('model should not be fetched'));
      }, function(err) {
        assert(err);
        done();
      });
  });

  it('should fetch all models', function(done) {
    collection
      .fetch()
      .then(function() {
        done();
      }, done);
  });

  it('should remove indexes when removing models', function(done) {
    function checkIndexes() {
      redis.keys('test:i:mymodel*', function(err, keys) {
        assert(keys.indexOf('test:i:mymodels:value:1') === -1);
        done();
      });
    }

    var model = collection.findWhere({id: 1});
    assert(model);
    model
      .destroy()
      .then(function() {
        checkIndexes();
      })
      .otherwise(done);
  });

  it('should update index when updating properties', function(done) {
    function checkNameIndex() {
      redis.smembers('test:i:mymodels:name:c', function(err, ids) {
        assert(ids.indexOf('3') === -1);
        assert(ids.indexOf('4') > -1);
        done();
      });
    }
    var model = collection.findWhere({id: 3});
    assert(model);
    model.set('name', 'e');
    model
      .save()
      .then(function() {
        redis.keys('test:i:mymodel*', function(err, keys) {
          assert(keys.indexOf('test:i:mymodels:name:e') > -1);
          checkNameIndex();
        });
      });
  });

  it('should remove reference to model in index after removing', function(done) {
    function checkPlatformIndex() {
      redis.smembers('test:i:mymodels:platforms:ios', function(err, ids) {
        // 1 & 2 were deleted, only 4 should be left
        assert(ids.length === 1);
        assert(ids.indexOf('4') > -1);
        done();
      });
    }
    function checkIndexes() {
      redis.keys('test:i:mymodel*', function(err, keys) {
        if (err) return done(err);
        assert(keys.indexOf('test:i:mymodels:value:2') > -1);
        assert(keys.indexOf('test:i:mymodels:name:b') === -1);
        redis.smembers('test:i:mymodels:value:2', function(err2, ids) {
          if (err2) return done(err2);
          assert(ids.indexOf('2') === -1);
          assert(ids.indexOf('4') > -1);
          checkPlatformIndex();
        });
      });
    }

    var model = collection.findWhere({id: 2});
    assert(model);
    assert(model.collection);
    model
      .destroy()
      .then(function() {
        checkIndexes();
      })
      .otherwise(done);
  });

  it('should create indexes when model\'s collection is not defined', function(done) {
    function checkIndexes() {
      redis.keys('test:i:mymodel*', function(err, keys) {
        assert(keys.indexOf('test:i:mymodels:name:x') > -1,
          'index test:i:mymodels:name:x was not created');
        done();
      });
    }
    var model = new IndexedModel({id: 7, value: 9, name: 'x', platforms: ['xx']});
    model
      .save()
      .then(function() {
        checkIndexes();
      }).otherwise(done);
  });

  it('should remove reference to model in index when collection is not defined', function(done) {
    function checkIndexes() {
      redis.keys('test:i:mymodel*', function(err, keys) {
        assert(keys.indexOf('test:i:mymodels:name:x') === -1);
        done();
      });
    }

    var model = new IndexedModel({id: 7});
    model
      .fetch()
      .then(function() {
        model
          .destroy()
          .then(function() {
            checkIndexes();
          }).otherwise(done);
      }).otherwise(done);
  });

  it('should not index model if dependency is not set', function(done) {
    function checkIndexes() {
      redis.keys('test:z:mymodel*', function(err, keys) {
        var key = 'test:z:mymodels:featured';
        assert(keys.indexOf(key) === -1);
        done();
      });
    }

    var model = new setup.IndexedByDateModel();
    model
      .save()
      .then(function() {
        checkIndexes();
      }).otherwise(done);

  });

  it('should index model, with score by date added', function(done) {
    function checkIndexes() {
      redis.keys('test:z:mymodel*', function(err, keys) {
        if (err) return done(err);
        var key = 'test:z:mymodels:featured';
        assert(keys.indexOf(key) > -1,
          'model should not be added to featured index when attribute is not set');
        redis.zrange(key, 0, -1, function(err2, ids) {
          if (err2) return done(err2);
          assert.equal(ids.length, 1);
          done();
        });
      });
    }

    var model = new setup.IndexedByDateModel({featured: true});
    model
      .save()
      .then(function() {
        checkIndexes();
      }).otherwise(done);
  });
});
