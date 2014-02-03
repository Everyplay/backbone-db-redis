var assert = require('assert');
var setup = require('./setup');
var MyModel = setup.MyModel;

describe('RedisDB', function() {
  describe('#Model', function() {
    it('should .save from store', function(t) {
      var m = new MyModel({id:1, "asd":"das"});
      m.save().then(function() {
        var m2 = new MyModel({id:1});
        m2.fetch().then(function() {
          assert.equal(m2.get("asd"),"das");
          t();
        });
      });
    });

    it('should .destroy from store', function(t) {
      var m = new MyModel({id:1, "asd":"das"});
      m.destroy()
        .then(function() {
          t();
        }).otherwise(t);
    });
  });
});