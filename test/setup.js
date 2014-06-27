var _ = require('lodash');
var RedisDb = require('../');
var Backbone = require('backbone');
var Promises = require('backbone-promises');
var Model = Promises.Model;
var Collection = Promises.Collection;
var nodefn = require('when/node/function');
var store = exports.store = new RedisDb('test');
var redis = require('redis');

var MyModel = exports.MyModel = Model.extend({
  db: store,
  sync: store.sync,
  type: 'mymodel',
  dbBaseKey: 'mymodels',
  url: function() {
    var key = this.dbBaseKey || this.type;
    if (!this.isNew()) {
      key += ':' + this.get(this.idAttribute);
    }
    return key;
  }
});

var MyCollection = exports.MyCollection = Collection.extend({
  db: store,
  sync: store.sync,
  model: MyModel,
  type: 'mymodels',
  url: function() {
    return this.type;
  }
});

var IndexedModel = exports.IndexedModel = MyModel.extend({
  indexes: [
    {property: 'value', sort: 'asc', unique: true},
    {property: 'name'},
    {property: 'platforms'}
  ]
});

var IndexedCollection = exports.IndexedCollection = MyCollection.extend({
  model: IndexedModel
});

var fixtures = [
  {id: 1, value: 1, name: 'a', platforms: ['android', 'ios']},
  {id: 2, value: 2, name: 'b', platforms: ['ios']},
  {id: 3, value: 3, name: 'c', platforms: ['android']},
  {id: 4, value: 2, name: 'c', platforms: ['ios']},
];

var IndexedByDateModel = exports.IndexedByDateModel = MyModel.extend({
  indexes: [
    {
      property: 'id',
      sort: function() {
        return Date.now();
      },
      dependencies: [
        {
          // add to index if attribute "featured" is set to true
          attribute: 'featured',
          value: true
        }
      ],
      key: 'z:mymodels:featured'
    }
  ]
});

var TestIndexedCollection = exports.TestIndexedCollection = MyCollection.extend({
  indexDb: store,
  indexKey: 'test:i:Foo:relation',

  indexSort: function(model) {
    return model.get('score');
  },

  /**
   * Adds a new model to the index. Only specified attribute is indexed.
   * Db adapter returns a Promise
   */
  addToIndex: function(model, options) {
    options = options ? _.clone(options) : {};
    if (!(model = this._prepareModel(model, options))) return false;
    if (!options.wait) this.add(model, options);
    return this._callAdapter('addToIndex', options, model);
  },

  /**
   * Read model ids from the index. Populates collection models with ids,
   * for fetching from the main storage.
   */
  readFromIndex: function(options) {
    return this._callAdapter('readFromIndex', options);
  },

  /**
   * Read from multiple indexes
   */
  readFromIndexes: function(options) {
    options = options ? _.clone(options) : {};
    options.indexKeys = this.indexKeys || options.indexKeys;
    options.unionKey = this.unionKey || options.unionKey;
    if (this.indexKey) options.indexKeys.push(this.indexKey);
    var args = [this, options];
    return nodefn.apply(_.bind(this.indexDb.readFromIndexes, this.indexDb), args);
  },

  /**
   * Removes a model from index
   */
  removeFromIndex: function(models, options) {
    if(!models) return false;
    this.remove(models, options);
    var singular = !_.isArray(models);
    models = singular ? [models] : _.clone(models);
    return this._callAdapter('removeFromIndex', options, models);
  },

  destroyAll: function(options) {
    return this._callAdapter('removeIndex', options);
  },

  /**
   *  Check if model exists in index
   */
  exists: function(model, options) {
    return this._callAdapter('existsInIndex', options, model);
  },

  /**
   * Get count of items in index
   */
  count: function(options) {
    return this._callAdapter('indexCount', options);
  },

  findKeys: function(keys, options) {
    options = options ? _.clone(options) : {};
    options.keys = keys;
    return this._callAdapter('findKeys', options);
  },

  _callAdapter: function(fn, options, models) {
    options = options ? _.clone(options) : {};
    if (!this.indexDb) {
      throw new Error('indexDb must be defined');
    }
    options.indexKey = this.indexKey;
    var args = [this, options];
    if (models) args.splice(1, 0, models);
    return nodefn.apply(_.bind(this.indexDb[fn], this.indexDb), args);
  }
});

exports.insertFixtureData = function (collection, cb) {
  var fns = [];
  _.each(fixtures, function(row) {
    fns.push(collection.create(row));
  });

  Promises.when.all(fns)
    .then(function() {
      cb(null);
    })
    .otherwise(function(err) {
      cb(err);
    });
};

exports.clearDb = function(cb) {
  var client = redis.createClient();
  client.keys('test*', function(err, keys) {
    keys.forEach(function(key) {
      client.del(key);
    });
    cb();
  });
};