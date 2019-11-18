import {chai} from 'meteor/practicalmeteor:chai';
import {sinon} from 'meteor/practicalmeteor:sinon';
import {Observable} from 'rxjs';
import {ObservableCursor, MongoObservable} from 'meteor-rxjs';




const expect = chai.expect;

describe('ObservableCursor', function () {
  let collection: Mongo.Collection<any>;
  let cursor: Mongo.Cursor<any>;
  let observable: ObservableCursor<any>;

  beforeEach(function () {
    collection = new Mongo.Collection(null);
    collection.allow({
      insert: function () {
        return true;
      },
      remove: function () {
        return true;
      },
      update: function () {
        return true;
      }
    });

    cursor = collection.find({});
    observable = ObservableCursor.create(cursor);
  });

  it('Should wrap the Mongo.Cursor and return RxJS Observable', () => {
    expect(observable instanceof Observable).to.equal(true);
  });

  it('Should not use the actual Cursor "observeChanges" method w/o Observable subscription', () => {
    let spy = sinon.spy(cursor, 'observeChanges');
    expect(spy.called).to.equal(false);
    spy.restore();
  });

  it('Should use the actual Cursor "observeChanges" after using Observable subscription', () => {
    let spy = sinon.spy(cursor, 'observeChanges');
    let subHandler = observable.subscribe();
    expect(spy.calledOnce).to.equal(true);
    spy.restore();
    subHandler.unsubscribe();
  });

  it('Should not trigger subscription callback when creating the subscription', () => {
    let spy = sinon.spy();
    let subscriptionHandler = observable.subscribe(spy);
    expect(spy.called).to.equal(false);
    subscriptionHandler.unsubscribe();
  });

  it('Subscription should unsubscribe after the unsubscribe call', () => {
    let subHandler;
    let callback = () => {
      subHandler.unsubscribe();
    };
    let spy = sinon.spy(callback);
    subHandler = observable.subscribe(spy);
    collection.insert({});
    collection.insert({});
    expect(spy.calledOnce).to.be.true;
  });

  it('Should trigger subscription callback when adding data to the collection', () => {
    let newDoc = {name: 'newDoc'};
    let subHandler;
    let callback = docs => {
      let inserted = docs[0];
      expect(inserted.name).to.equal(newDoc.name);
      subHandler.unsubscribe();
    };
    let spy = sinon.spy(callback);
    subHandler = observable.subscribe(spy);
    collection.insert(newDoc);
    expect(spy.calledOnce).to.be.true;
  });

  it('Should trigger subscription callback when moving items in the collection', (done) => {
    cursor = collection.find({}, {sort: {name: 1}});
    observable = ObservableCursor.create(cursor);

    let newDoc = {name: 'ZZZZ'};
    let subHandler;
    let count = 0;

    let callback = docs => {
      count++;

      // 4 because: insert, insert, update, *move*
      console.log(count);
      if (count === 4) {
        let firstItem = docs[0];
        expect(firstItem.name).to.equal('AAAA');
        subHandler.unsubscribe();
        done();
      }
    };

    subHandler = observable.subscribe(callback);

    let objectId = collection.insert(newDoc);

    collection.insert({
      name: 'BBBB'
    });

    collection.update({_id: objectId}, { $set: {name: 'AAAA'} });
  });

  it('Should trigger callback twice when inserting a doc and then removing it', () => {
    let count = 0;
    let subHandler;
    let callback = docs => {
      count++;
      if (count == 2) {
        expect(docs.length).to.equal(0);
        subHandler.unsubscribe();
      }
    };
    let spy = sinon.spy(callback);
    let subHandler = observable.subscribe(spy);
    let idToRemove = collection.insert({test: true});
    collection.remove(idToRemove);
    expect(spy.calledTwice).to.be.true;
  });

  it('Should subscription callback should have updated docs after updating', done => {
    let count = 0;
    let callback = docs => {
      count++;
      if (count == 1) {
        expect(docs[0].test).to.equal(true);
      }

      if (count == 2) {
        expect(docs[0].test).to.equal(false);
        subHandler.unsubscribe();
        done();
      }
    };
    let spy = sinon.spy(callback);

    let subHandler = observable.subscribe(spy);
    let idToUpdate = collection.insert({test: true});
    collection.update({_id: idToUpdate}, {$set: {test: false}});
    expect(spy.calledTwice).to.be.true;
  });

  it('Should not alias subscription callback parameter', done => {
    let count = 0;
    let firstDocs;
    let callback = docs => {
      count++;

      function valueSet(docs) { return docs.map((doc) => doc.value).sort().join(); }
      if (count == 1) {
        expect(valueSet(docs)).to.equal('1');
        firstDocs = docs;
      }

      if (count == 2) {
        expect(valueSet(firstDocs)).to.equal('1');
        expect(valueSet(docs)).to.equal('1,2');
        subHandler.unsubscribe();
        done();
      }
    };
    let spy = sinon.spy(callback);

    let subHandler = observable.subscribe(spy);
    collection.insert({value: 1});
    collection.insert({value: 2});
    expect(spy.calledTwice).to.be.true;
  });

  it('Should stop Mongo cursor when the last subscription unsubscribes', () => {
    let stopSpy = sinon.spy();
    let spy = sinon.stub(cursor, 'observeChanges', () => {
      return {
        stop: stopSpy
      }
    });

    let subHandler = observable.subscribe();
    subHandler.unsubscribe();

    expect(stopSpy.callCount).to.equal(1);
    spy.restore();
  });

  it('RxJS operators should persist', () => {
    expect(observable.count).to.equal(Observable.prototype.count);
    expect(observable.map).to.equal(Observable.prototype.map);
  });

  it('Should trigger collectionCount when adding item', () => {
    let newDoc = {name: 'newDoc'};
    let subHandler, subCountHandler;
    let callback = count => {
      expect(count).to.equal(1);
      subHandler.unsubscribe();
      subCountHandler.unsubscribe();
    };

    subHandler = observable.subscribe();
    subCountHandler = observable.collectionCount().subscribe(callback);
    collection.insert(newDoc);
  });

  it('Should trigger collectionCount when adding and removing items', (done) => {
    let newDoc = {name: 'newDoc'};
    let subHandler, subCountHandler;
    let c = 0;

    let callback = count => {
      if (c === 0) {
        expect(count).to.equal(1);
      }
      else if (c === 1) {
        expect(count).to.equal(0);
        subHandler.unsubscribe();
        subCountHandler.unsubscribe();
        done();
      }

      c++;
    };

    subHandler = observable.subscribe();
    subCountHandler = observable.collectionCount().subscribe(callback);
    let id = collection.insert(newDoc);
    collection.remove({_id: id});
  });

  it('Multiple subscription for the same Observable should replay last value', () => {
    let wrappedCollection = MongoObservable.fromExisting(collection);
    let observable = wrappedCollection.find({});

    let spyCb1 = sinon.spy();
    let spyCb2 = sinon.spy();
    let firstSubscriptionHandler = observable.subscribe(spyCb1);
    wrappedCollection.insert({test: 1});
    wrappedCollection.insert({test: 2});
    wrappedCollection.insert({test: 3});
    let secondSubscriptionHandler = observable.subscribe(spyCb2);
    wrappedCollection.insert({test: 4});

    expect(spyCb1.callCount).to.equal(4);
    expect(spyCb2.callCount).to.equal(1);

    firstSubscriptionHandler.unsubscribe();
    secondSubscriptionHandler.unsubscribe();
  });
});
