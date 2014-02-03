var debug = require('debug')('backbone-db-redis:indexing');
var _ = require('underscore');

function getKey(db, baseKey, key, val) {
  return db.name + ':i:' + baseKey + ':' + key + ':' + val;
}

function addToSet(set, member){
  return ['SADD', set, member];
}

function removeFromSet(set, member) {
  return ['SREM', set, member];
}

function addToSortedSet(set, member, score) {
  debug('addToSortedSet', set, member, score);
  return ['ZADD', set, score, member];
}

function removeFromSortedSet(set, member) {
  return ['ZREM', set, member];
}

/**
params:
  options object:
  {
    indexes - array of properties to be indexed
    data - model.attributes
    prevData - model's previous attributes
    baseKey - model's identification string
    id - model's id
    db - RedisDb instance
  }
**/
exports.updateIndexes = function (options, callback) {
  var queue = [];
  var indexes = options.indexes;
  var data = options.data;
  var prevData = options.prevData;
  var baseKey = options.baseKey;
  var id = options.id;
  var db = options.db;
  var operation = options.operation;
  var vals;
  var dbKey;

  indexes.forEach(function (index) {
    // does the data meet dependency for the index,
    // i.e. should this model attribute be indexed
    var meetsDependencies = function() {
      var dependencies = index.dependencies;
      if(!dependencies || !dependencies.length) return true;
      return _.all(dependencies, function(dependency) {
        return data.hasOwnProperty(dependency.attribute) &&
          (data[dependency.attribute] === dependency.value);
      });
    };

    var key = index.property;
    var sort = index.sort;
    var postFix = ':' + (index.key || 'i:' + baseKey + ':' + key);
    var sortKey = db.name + postFix;
    if(operation === 'add' && data.hasOwnProperty(key)) {
      vals = data[key];
      if(!_.isArray(vals)) vals = [vals];
      _.each(vals, function(val) {
        dbKey = getKey(db, baseKey, key, val);
        if(sort && meetsDependencies()) {
          var score = _.isFunction(sort) ? sort() : val;
          queue.push(addToSortedSet(sortKey, id, score));
        }
        queue.push(addToSet(dbKey, id));
      });
    }
    // value was changed or deleting object
    if((operation === 'add' && prevData && prevData[key] && prevData[key] !== data[key]) ||
      (operation === 'delete' && prevData && prevData.hasOwnProperty(key))) {
      vals = prevData[key];
      if(!_.isArray(vals)) vals = [vals];
      _.each(vals, function(val) {
        dbKey = getKey(db, baseKey, key, val);
        if(sort) queue.push(removeFromSortedSet(sortKey, id, 1));
        queue.push(removeFromSet(dbKey, id));
      });
    }
  });

  if(queue.length) {
    debug('updating indexes:', queue);
    db.redis
      .multi(queue)
      .exec(function (err) {
        callback(err, data);
      });
  } else {
    debug('no indexes need updating');
    callback(null);
  }
};
