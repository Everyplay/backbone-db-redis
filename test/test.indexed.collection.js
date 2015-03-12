var assert = require('assert');
var _ = require('lodash' );
var Promises = require('backbone-promises');
var when = Promises.when;
var sequence = require('when/sequence');
var setup = require('./setup');
var TestCollection = setup.TestIndexedCollection;

var TestCollection2 = TestCollection.extend({
  indexKey: 'test:i:Foo:relation2'
});

describe('Test IndexedCollection', function () {
  var collection;
  var collection2;
  var scoreConversion = {
    fn: function(score) {
      return score;
    },
    attribute: 'score'
  };

  before(function() {
    collection = new TestCollection();
    var fns = [
      collection.create({data: 'aaa', score: 22}),
      collection.create({data: 'bbb', score: 1}),
      collection.create({data: 'ccc', score: 99})
    ];
    return when.all(fns)
      .then(function() {
        return sequence([
          _.bind(collection.addToIndex, collection, collection.at(0)),
          _.bind(collection.addToIndex, collection, collection.at(1)),
          _.bind(collection.addToIndex, collection, collection.at(2))
        ]);
      });
  });

  after(function(done) {
    setup.clearDb(done);
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

  it('should read from index with min range', function(done) {
    var opts = {
      score: {
        min: '(22',
        conversion: scoreConversion
      }
    };
    collection = new TestCollection();
    collection
      .readFromIndex(opts)
      .then(function() {
        assert.equal(collection.length, 1);
        assert.equal(collection.at(0).get('score'), '99');
        done();
      }).otherwise(done);
  });

  it('should read ids with given score options', function(done) {
    var opts = {
      score: {
        min: 22,
        max: 999,
        // defines how to format scores into model
        conversion: scoreConversion
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

  it('should read ids from index in reverse', function(done) {
    collection = new TestCollection();
    collection
      .readFromIndex({sortOrder: 1})
      .then(function() {
        assert.equal(collection.length, 3);
        assert.equal(collection.pluck('id').length, 3);
        collection.at(0).id.should.equal('2');
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
        collection.at(0).id.should.equal('3');
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
        conversion: scoreConversion
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

  describe('before_id & after_id', function() {
    it('should read ids with after_id', function() {
      var opts = {
        after_id: '1',
        score: {
          conversion: scoreConversion
        }
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).get('score'), '1');
        });
    });

    it('should read ids with before_id', function() {
      var opts = {
        before_id: '1'
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '3');
        });
    });

    it('should read ids with after_id & checking the order', function() {
      var opts = {
        after_id: '2' // 2 has the lowest score
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 0);
        });
    });

    it('should read ids with after_id & checking the order', function() {
      var opts = {
        after_id: '3' // 2 has the highest score
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 2);
          assert.equal(collection.at(0).id, '1'); // score 22
          assert.equal(collection.at(1).id, '2'); // score 1
        });
    });

    it('should read ids with before_id  3 & checking the order', function() {
      var opts = {
        before_id: '3' // has the highest score
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 0);
        });
    });

    it('should read ids with before_id 2 & checking the order', function() {
      var opts = {
        before_id: '2' // 2 has the lowest score
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 2);
          assert.equal(collection.at(0).id, '3'); // score 99
          assert.equal(collection.at(1).id, '1'); // score 22
        });
    });

    it('should read ids with before_id 1 & checking the order', function() {
      var opts = {
        before_id: '1'
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '3'); // score 99
        });
    });

    it('should read ids with after_id & limit', function() {
      var opts = {
        after_id: '3',
        limit: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '1');
        });
    });

    it('should read ids with after_id 2 when sorted ascending', function() {
      var opts = {
        after_id: '2', // 2 has the lowest score
        sortOrder: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 2);
          assert.equal(collection.at(0).id, '1'); // score 22
          assert.equal(collection.at(1).id, '3'); // score 99
        });
    });

    it('should read ids with after_id 2 & limit when sorted ascending', function() {
      var opts = {
        after_id: '2', // 2 has the lowest score
        sortOrder: 1,
        limit: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '1'); // score 22
        });
    });

    it('should read ids with before_id 2 when sorted ascending', function() {
      var opts = {
        before_id: '2', // 2 has the lowest score
        sortOrder: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 0);
        });
    });

    it('should read ids with before_id 3 when sorted ascending', function() {
      var opts = {
        before_id: '3', // 3 has the highest score
        sortOrder: 1,
        score: {
          conversion: scoreConversion
        }
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 2);
          assert.equal(collection.at(0).id, '2'); // score 1
          assert.equal(collection.at(1).id, '1'); // score 22
        });
    });

    it('should read ids with before_id 3 & limit when sorted ascending', function() {
      var opts = {
        before_id: '3', // 3 has the highest score, i.e. the last item
        sortOrder: 1,
        score: {
          conversion: scoreConversion
        },
        limit: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '1'); // score 22
        });
    });

    it('should read ids with before_id 3 & limit when sorted ascending', function() {
      var opts = {
        before_id: '1', // 1 has the 2nd highest score 22
        sortOrder: 1,
        score: {
          conversion: scoreConversion
        },
        limit: 1
      };
      collection = new TestCollection();
      return collection
        .readFromIndex(opts)
        .then(function() {
          assert.equal(collection.length, 1);
          assert.equal(collection.at(0).id, '2'); // score 1
        });
    });
  });

});
