## Usage

```js

var store = new RedisDb('mymodel', redis.createClient());

var MyModel = Model.extend({
  db: store,
  sync: RedisDb.sync,
  url: function() {
    var key = 'mymodel';
    if(!this.isNew()) {
      key += ':' + this.id;
    }
    return key;
  }
});

var a = new MyModel({id:"1"});

a.fetch().done(function(model) {
  console.log(a);
});
```

### Indexing:

Model can define Array typed property "indexes", which defines which attributes are indexed. Example:

```js
var MyModel = Model.extend({
  indexes: [
    {
      property: 'value', 
      sort: 'asc'
    },
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

```

#### property

Model's attribute to be indexed

### sort

If defined attribute index is stored in sorted set. If defined as function, score will be calculated with given function. If not defined attribute is indexed in unordered set.

### dependencies

Index attribute only if defined dependencies are met.

### key

If defined, store index in given set key, otherwise created automatically.