<a name="Collection"></a>

## Collection
A class represents a MongoDB collection in the client side, wrapped with RxJS
Observables, so you can use it with your Angular 2 easier.
The wrapper has the same API as Mongo.Collection, only the "find" method returns
an ObservableCursor instead of regular Mongo.Cursor.

T is a generic type - should be used with the type of the objects inside the collection.

**Kind**: global class  

* [Collection](#Collection)
    * [new Collection(nameOrExisting, options)](#new_Collection_new)
    * _instance_
        * [.collection](#Collection+collection) ⇒ <code>Mongo.Collection.&lt;T&gt;</code>
        * [.allow()](#Collection+allow) ⇒ <code>Boolean</code>
        * [.deny()](#Collection+deny) ⇒ <code>Boolean</code>
        * [.rawCollection()](#Collection+rawCollection) ⇒ <code>Mongo.Collection</code>
        * [.rawDatabase()](#Collection+rawDatabase) ⇒ <code>Mongo.Db</code>
        * [.insert(doc)](#Collection+insert) ⇒ <code>Observable.&lt;string&gt;</code>
        * [.remove(selector)](#Collection+remove) ⇒ <code>Observable.&lt;Number&gt;</code>
        * [.update(selector, modifier, options)](#Collection+update) ⇒ <code>Observable.&lt;Number&gt;</code>
        * [.upsert(selector, modifier, options)](#Collection+upsert) ⇒ <code>Observable.&lt;{numberAffected, insertedId}&gt;</code>
        * [.find(selector, options)](#Collection+find) ⇒ <code>ObservableCursor.&lt;T&gt;</code>
        * [.findOne(selector, options)](#Collection+findOne) ⇒ <code>any</code>
    * _inner_
        * [~MongoQueryOptions](#Collection..MongoQueryOptions) : <code>Object</code>
        * [~MongoUpsertOptions](#Collection..MongoUpsertOptions) : <code>Object</code>
        * [~MongoUpdateOptions](#Collection..MongoUpdateOptions) : <code>Object</code>

<a name="new_Collection_new"></a>

### new Collection(nameOrExisting, options)
Creates a new Mongo.Collection instance wrapped with Observable features.


| Param | Type | Description |
| --- | --- | --- |
| nameOrExisting | <code>String</code> \| <code>Mongo.Collection</code> | The name of the collection. If null, creates an  unmanaged (unsynchronized) local collection. If provided an instance of existing collection, will  create a wrapper for the existing Mongo.Collection. |
| options | <code>ConstructorOptions</code> | Creation options. |

<a name="Collection+collection"></a>

### collection.collection ⇒ <code>Mongo.Collection.&lt;T&gt;</code>
Returns the Mongo.Collection object that wrapped with the MongoObservable.Collection.

**Kind**: instance property of [<code>Collection</code>](#Collection)  
**Returns**: <code>Mongo.Collection.&lt;T&gt;</code> - The Collection instance  
<a name="Collection+allow"></a>

### collection.allow() ⇒ <code>Boolean</code>
Allow users to write directly to this collection from client code, subject to limitations you define.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
<a name="Collection+deny"></a>

### collection.deny() ⇒ <code>Boolean</code>
Override allow rules.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
<a name="Collection+rawCollection"></a>

### collection.rawCollection() ⇒ <code>Mongo.Collection</code>
Returns the Collection object corresponding to this collection from the npm
 mongodb driver module which is wrapped by Mongo.Collection.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Mongo.Collection</code> - The Collection instance  
**See**: [rawCollection on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-rawCollection)  
<a name="Collection+rawDatabase"></a>

### collection.rawDatabase() ⇒ <code>Mongo.Db</code>
Returns the Db object corresponding to this collection's database connection from the
 npm mongodb driver module which is wrapped by Mongo.Collection.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Mongo.Db</code> - The Db instance  
**See**: [rawDatabase on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-rawDatabase)  
<a name="Collection+insert"></a>

### collection.insert(doc) ⇒ <code>Observable.&lt;string&gt;</code>
Insert a document in the collection.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Observable.&lt;string&gt;</code> - Observable which completes with the inserted ObjectId  
**See**: [insert on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-insert)  

| Param | Type | Description |
| --- | --- | --- |
| doc | <code>T</code> | The document to insert. May not yet have an _id  attribute, in which case Meteor will generate one for you. |

<a name="Collection+remove"></a>

### collection.remove(selector) ⇒ <code>Observable.&lt;Number&gt;</code>
Remove documents from the collection.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Observable.&lt;Number&gt;</code> - Observable which completes with the number of affected rows  
**See**: [remove on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-remove)  

| Param | Type | Description |
| --- | --- | --- |
| selector | [<code>Selector</code>](#Collection..MongoQueryMongo.Selector) | Specifies which documents to modify |

<a name="Collection+update"></a>

### collection.update(selector, modifier, options) ⇒ <code>Observable.&lt;Number&gt;</code>
Modify one or more documents in the collection.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Observable.&lt;Number&gt;</code> - Observable which completes with the number of affected rows  
**See**: [update on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-update)  

| Param | Type | Description |
| --- | --- | --- |
| selector | [<code>Selector</code>](#Collection..MongoQueryMongo.Selector) | Specifies which documents to modify |
| modifier | <code>Modifier</code> | Specifies how to modify the documents |
| options | <code>MongoUpdateOptions</code> | Update options  first argument and, if no error, the number of affected documents as the second |

<a name="Collection+upsert"></a>

### collection.upsert(selector, modifier, options) ⇒ <code>Observable.&lt;{numberAffected, insertedId}&gt;</code>
Finds the first document that matches the selector, as ordered by sort and skip options.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>Observable.&lt;{numberAffected, insertedId}&gt;</code> - Observable which completes with an
 Object that contain the keys numberAffected and insertedId.  
**See**: [upsert on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-upsert)  

| Param | Type | Description |
| --- | --- | --- |
| selector | [<code>Selector</code>](#Collection..MongoQueryMongo.Selector) | Specifies which documents to modify |
| modifier | <code>Modifier</code> | Specifies how to modify the documents |
| options | <code>MongoUpsertOptions</code> | Upsert options  first argument and, if no error, the number of affected documents as the second. |

<a name="Collection+find"></a>

### collection.find(selector, options) ⇒ <code>ObservableCursor.&lt;T&gt;</code>
Method has the same notation as Mongo.Collection.find, only returns Observable.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>ObservableCursor.&lt;T&gt;</code> - RxJS Observable wrapped with Meteor features.  
**See**: [find on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-find)  

| Param | Type | Description |
| --- | --- | --- |
| selector | [<code>Selector</code>](#Collection..MongoQueryMongo.Selector) | A query describing the documents to find |
| options | [<code>MongoQueryOptions</code>](#Collection..MongoQueryOptions) | Query options, such as sort, limit, etc. |

**Example** *(Using Angular2 Component)*  
```js
 const MyCollection = MongoObservable.Collection("myCollection");

 class MyComponent  {
    private myData: ObservableCursor<any>;

    constructor() {
       this.myData = MyCollection.find({}, {limit: 10});
    }
 }
```
<a name="Collection+findOne"></a>

### collection.findOne(selector, options) ⇒ <code>any</code>
Finds the first document that matches the selector, as ordered by sort and skip options.

**Kind**: instance method of [<code>Collection</code>](#Collection)  
**Returns**: <code>any</code> - The first object, or `undefined` in case of non-existing object.  
**See**: [findOne on Meteor documentation](https://docs.meteor.com/api/collections.html#Mongo-Collection-findOne)  

| Param | Type | Description |
| --- | --- | --- |
| selector | [<code>Selector</code>](#Collection..MongoQueryMongo.Selector) | A query describing the documents to find |
| options | [<code>MongoQueryOptions</code>](#Collection..MongoQueryOptions) | Query options, such as sort, limit, etc. |

<a name="Collection..MongoQueryOptions"></a>

### Collection~MongoQueryOptions : <code>Object</code>
An options object for MongoDB queries.

**Kind**: inner typedef of [<code>Collection</code>](#Collection)  
**Properties**

| Name | Type | Description |
| --- | --- | --- |
| sort | <code>Object</code> | Sort order (default: natural order) |
| skip | <code>Number</code> | Number of results to skip at the beginning |
| fields | <code>Object</code> | Dictionary of fields to return or exclude. |
| reactive | <code>Boolean</code> | (Client only) Default true; pass false to disable reactivity |
| transform | <code>function</code> | Overrides transform on the Collection for this cursor. Pass null to disable transformation. |

<a name="Collection..MongoUpsertOptions"></a>

### Collection~MongoUpsertOptions : <code>Object</code>
A MongoDB query options for upsert action

**Kind**: inner typedef of [<code>Collection</code>](#Collection)  
**Properties**

| Name | Type | Description |
| --- | --- | --- |
| multi | <code>Boolean</code> | True to modify all matching documents; false to only modify one of the matching documents (the default). |

<a name="Collection..MongoUpdateOptions"></a>

### Collection~MongoUpdateOptions : <code>Object</code>
A MongoDB query options for update action

**Kind**: inner typedef of [<code>Collection</code>](#Collection)  
**Properties**

| Name | Type | Description |
| --- | --- | --- |
| multi | <code>Boolean</code> | True to modify all matching documents; |
| upsert | <code>Boolean</code> | True to use upsert logic. |

