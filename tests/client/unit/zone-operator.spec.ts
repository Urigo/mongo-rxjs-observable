import * as chai from 'chai';
import * as sinon from 'sinon';
import { Observable } from 'rxjs';
import { MeteorObservable, MongoObservable, zoneOperator } from 'meteor-rxjs';

import 'zone.js/dist/zone.js';

const expect = chai.expect;

describe('ZoneOperator', () => {
  let observable = new MongoObservable.Collection(null);
  observable.allow({
    insert: function () {
      return true;
    }
  });

  it('Should run in the expected zone on the next', done => {
    let gZone = Zone.current;
    let zone = Zone.current.fork({ name: 'ng'});

    let obs: Observable<any> = Observable.create(observer => {
      gZone.run(() => observer.next());
    });
    zone.run(() => {
      obs.pipe(zoneOperator()).subscribe(() => {
        expect(Zone.current).to.equal(zone);
        done();
      });
    });
  });

  it('Zone operator should use propagated zone from the autorun observable',
    done => {
      let zone = Zone.current.fork({ name: 'ng'});
      zone.run(() => {
        let subHandler = MeteorObservable.autorun().subscribe(() => {
          console.log(subHandler);
          observable.find({}).pipe(zoneOperator()).subscribe(() => {
            expect(Zone.current).to.equal(zone);
            subHandler.unsubscribe();
            done();
          });
        });
      });
      observable.insert({});
    });
});
