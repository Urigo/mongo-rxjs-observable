import * as chai from 'chai';
import * as sinon from 'sinon';
import { Meteor } from 'meteor/meteor';
import { MeteorObservable } from 'meteor-rxjs';
import { Observable, isObservable } from 'rxjs';

const expect = chai.expect;

describe('MeteorObservable', () => {
  describe('call', () => {
    it('Should return RxJS Observable when using "call"', () => {
      let returnValue = MeteorObservable.call('testMethod');
      expect(isObservable(returnValue)).to.equal(true);
    });

    it('Should NOT run the actual "call" method without subscribing to the result', () => {
      let spy = sinon.spy(Meteor, 'call');
      MeteorObservable.call('testMethod');
      expect(spy.called).to.equal(false);
      spy.restore();
    });

    it('Should run the actual "call" method when subscribing to the result', () => {
      let spy = sinon.spy(Meteor, 'call');
      let subHandler = MeteorObservable.call('testMethod').subscribe();
      expect(spy.calledOnce).to.equal(true);
      spy.restore();
      subHandler.unsubscribe();
    });

    it('Should trigger the RxJS Observable "next" callback when got the server response',
      (done) => {
        let subHandler = MeteorObservable.call('testMethod').subscribe((serverResponse) => {
          expect(serverResponse).to.equal('TEST_VALUE');
          subHandler.unsubscribe();
          done();
        });
      });

    it('Should trigger the RxJS Observable "error" callback when got the server error',
      (done) => {
        let subscriptionHandler = MeteorObservable.call('NON_EXISTING_METHOD').subscribe(null,
          (e) => {
            expect(e instanceof Meteor.Error).to.equal(true);
            subscriptionHandler.unsubscribe();
            done();
          });
      });
  });

  describe('subscribe', () => {
    function getSubsCount() {
      return Object.keys((<any>Meteor).default_connection._subscriptions).length;
    }

    it('Should return RxJS Observable when using "subscribe"', () => {
      let returnValue = MeteorObservable.subscribe('test');
      expect(isObservable(returnValue)).to.equal(true);
    });

    it('Should NOT run the actual "subscribe" method without subscribing to the result', () => {
      let spy = sinon.spy(Meteor, 'subscribe');
      MeteorObservable.subscribe('test');
      expect(spy.called).to.equal(false);
      spy.restore();
    });

    it('Should run the actual "subscribe" method when subscribing to the result', () => {
      let spy = sinon.spy(Meteor, 'subscribe');
      let subHandler = MeteorObservable.subscribe('test').subscribe();
      expect(spy.called).to.equal(true);
      spy.restore();
      subHandler.unsubscribe();
    });

    it('Should call RxJS Observable "next" callback when subscription is ready', done => {
      let subHandler = MeteorObservable.subscribe('test').subscribe(() => {
        subHandler.unsubscribe();
        done();
      });
    });

    it('Should stop subscription when one observer subscribes', done => {
      let baseCount = getSubsCount();
      let subHandler = MeteorObservable.subscribe('test').subscribe(() => {
        expect(getSubsCount()).to.equal(baseCount + 1);
        subHandler.unsubscribe();
        expect(getSubsCount()).to.equal(baseCount);
        done();
      });
    });

    it('Should persist same subscription when two observers subscribe, and then one unsubscribes',
      done => {
        let baseCount = getSubsCount();
        let observable = MeteorObservable.subscribe('test');
        let subHandler1 = observable.subscribe(() => { });
        let subHandler2 = observable.subscribe(() => {
          expect(getSubsCount()).to.equal(baseCount + 1);
          subHandler1.unsubscribe();
          expect(getSubsCount()).to.equal(baseCount + 1);
          subHandler2.unsubscribe();
          expect(getSubsCount()).to.equal(baseCount);
          done();
        });
      });
  });
});
