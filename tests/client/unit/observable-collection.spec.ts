import * as chai from 'chai';
import * as sinon from 'sinon';
import { MongoObservable, ObservableCursor } from 'meteor-rxjs';
import { Observable, isObservable } from 'rxjs';

const expect = chai.expect;

describe('MongoObservable methods bridge', () => {
  let observable = new MongoObservable.Collection(null);
  let mongoCollection = observable.collection;

  it('Should return RxJS Observable object when using "find"', () => {
    let findResult = observable.find({});
    expect(isObservable(findResult)).to.equal(true);
  });

  it('Should wrap existing collection', () => {
    let collection = new Mongo.Collection(null);
    let insert = sinon.stub(collection, 'insert');

    let observable = new MongoObservable.Collection(collection);
    observable.insert({});
    expect(insert.calledOnce).to.be.true;
  });

  it('Insert should return an observable', done => {
    observable.insert({}).subscribe(id => {
      expect(id).to.be.string;
      done();
    });
  });

  it('Remove should return an observable', done => {
    observable.insert({}).subscribe(id => {
      observable.remove(id).subscribe(() => {
        done();
      });
    });
  });

  function testOriginalMethod(methodName) {
    it(`Should call the original ${methodName} method of the Mongo.Collection`, () => {
      let stub = sinon.stub(mongoCollection, methodName);
      observable[methodName]({});
      expect(stub.calledOnce).to.equal(true);
    });
  }

  testOriginalMethod('allow');
  testOriginalMethod('deny');
  testOriginalMethod('insert');
  testOriginalMethod('rawCollection');
  testOriginalMethod('rawDatabase');
  testOriginalMethod('remove');
  testOriginalMethod('update');
  testOriginalMethod('upsert');
  testOriginalMethod('find');
  testOriginalMethod('findOne');
});
