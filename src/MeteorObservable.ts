import { Observable, Subscriber } from 'rxjs';

import { isMeteorCallbacks, forkZone, removeObserver } from './utils';

let liveSubscriptions = [];

function throwInvalidCallback(method: string) {
  throw new Error(
    `Invalid ${method} arguments:
     your last param can't be a callback function,
     please remove it and use ".subscribe" of the Observable!`);
}

/**
 * This is a class with static methods that wrap Meteor's API and return RxJS
 * Observables. The methods' signatures are the same as Meteor's, with the ]
 * exception that the callbacks are handled by Meteor-rxjs. Instead of
 * providing callbacks, you need to subscribe to the observables that are
 * returned. The methods that are wrapped in MeteorObservable are
 * [Meteor.call](https://docs.meteor.com/api/methods.html#Meteor-call),
 * [Meteor.autorun](https://docs.meteor.com/api/tracker.html#Tracker-autorun)
 * and [Meteor.subscribe](https://docs.meteor.com/api/pubsub.html#Meteor-subscribe).
 */
export class MeteorObservable {

  /**
   * Invokes a [Meteor Method](https://docs.meteor.com/api/methods.html)
   * defined on the server, passing any number of arguments. This method has
   * the same signature as
   * [Meteor.call](https://docs.meteor.com/api/methods.html#Meteor-call), only
   * without the callbacks:
   *    MeteorObservable.call(name, [...args])
   *
   *
   *  @param {string} name - Name of the method in the Meteor server
   *  @param {any} args - Parameters that will be forwarded to the method.
   *   after the func call to initiate change detection.
   *  @returns {Observable<T>} - RxJS Observable, which completes when the
   *  server returns a response.
   *
   *  @example <caption>Example using Angular2 Component</caption>
   *  class MyComponent  {
   *     constructor() {
   *
   *     }
   *
   *     doAction(payload) {
   *        MeteorObservable.call("myData", payload).subscribe((response) => {
   *           // Handle success and response from server!
   *        }, (err) => {
   *          // Handle error
   *        });
   *     }
   *  }
   */
  public static call<T>(name: string, ...args: any[]): Observable<T> {
    const lastParam = args[args.length - 1];

    if (isMeteorCallbacks(lastParam)) {
      throwInvalidCallback('MeteorObservable.call');
    }

    let zone = forkZone();

    return Observable.create((observer: Subscriber<Meteor.Error | T>) => {
      Meteor.call(name, ...args.concat([
        (error: Meteor.Error, result: T) => {
          zone.run(() => {
            error ? observer.error(error) :
              observer.next(result);
            observer.complete();
          });
        }
      ]));
    });
  }

  /**
   * When you subscribe to a collection, it tells the server to send records to
   * the client. This method has the same signature as
   * [Meteor.subscribe](https://docs.meteor.com/api/pubsub.html#Meteor-subscribe),
   * except without the callbacks again:
   *    subscribe(name, [...args])
   *
   *  You can use this method from any Angular2 element - such as Component,
   *  Pipe or Service.
   *
   *  @param {string} name - Name of the publication in the Meteor server
   *  @param {any} args - Parameters that will be forwarded to the publication.
   *   after the func call to initiate change detection.
   *  @returns {Observable} - RxJS Observable, which completes when the
   *  subscription is ready.
   *
   *  @example <caption>Example using Angular2 Service</caption>
   *  class MyService {
   *     private meteorSubscription: Observable<any>;
   *
   *     constructor() {
   *
   *     }
   *
   *     subscribeToData() {
   *        this.meteorSubscription = MeteorObservable.subscribe<any>("myData").subscribe(() => {
   *           // Subscription is ready!
   *        });
   *     }
   *
   *     unsubscribeToData() {
   *        this.meteorSubscription.unsubscribe();
   *     }
   *  }
   *
   *  @example <caption>Example using Angular2 Component</caption>
   *  class MyComponent implements OnInit, OnDestroy {
   *     private meteorSubscription: Observable<any>;
   *
   *     constructor() {
   *
   *     }
   *
   *     ngOnInit() {
   *        this.meteorSubscription = MeteorObservable.subscribe("myData").subscribe(() => {
   *           // Subscription is ready!
   *        });
   *     }
   *
   *     ngOnDestroy() {
   *        this.meteorSubscription.unsubscribe();
   *     }
   *  }
   *
   *  @see {@link http://docs.meteor.com/api/pubsub.html|Publications in Meteor documentation}
   */
  public static subscribe<T>(name: string, ...args: any[]): Observable<T> {
    let lastParam = args[args.length - 1];

    if (isMeteorCallbacks(lastParam)) {
      throwInvalidCallback('MeteorObservable.subscribe');
    }

    let zone = forkZone();
    let observers = [];
    let subscribe = () => {
      return Meteor.subscribe(name, ...args.concat([{
        onError: (error: Meteor.Error) => {
          zone.run(() => {
            observers.forEach(observer => observer.error(error));
          });
        },
        onReady: () => {
          zone.run(() => {
            observers.forEach(observer => observer.next());
          });
        }
      }
      ]));
    };

    let subHandler = null;
    return Observable.create((observer: Subscriber<Meteor.Error | T>) => {
      observers.push(observer);
      // Execute subscribe lazily.
      if (subHandler === null) {
        subHandler = subscribe();
        if (liveSubscriptions.find(sub => sub === subHandler.subscriptionId)) {
          // subscription already exists, call observer.next() since Meteor won't.
          observer.next();
        } else {
          liveSubscriptions.push(subHandler.subscriptionId);
        }
      }
      return () => {
        removeObserver(observers,
          observer, () => {
            // remove subscription from liveSubscriptions list
            let i = liveSubscriptions.findIndex(
              sub => sub === subHandler.subscriptionId
            );

            if (i > -1) {
              liveSubscriptions.splice(i, 1);
            }

            subHandler.stop();

          });
      };
    });
  }

  /**
   * Allows you to run a function every time there is a change is a reactive
   * data sources. This method has the same signature as
   * [Meteor.autorun](https://docs.meteor.com/api/tracker.html#Tracker-autorun),
   * only without the callback:
   *    MeteorObservable.autorun()
   *
   *  @returns {Observable<T>} - RxJS Observable, which trigger the subscription callback
   *  each time that Meteor Tracker detects a change.
   *  @example <caption>Example using Angular2 Component</caption>
   *  class MyComponent  {
   *     constructor() {
   *
   *     }
   *
   *     doAction(payload) {
   *        MeteorObservable.autorun().subscribe(() => {
   *           // Handle Tracker autorun change
   *        });
   *     }
   *  }
   */
  public static autorun(): Observable<Tracker.Computation> {
    let zone = forkZone();
    let observers = [];
    let autorun = () => {
      return Tracker.autorun((computation: Tracker.Computation) => {
        zone.run(() => {
          observers.forEach(observer => observer.next(computation));
        });
      });
    };

    let handler = null;
    return Observable.create((observer: Subscriber<Meteor.Error | Tracker.Computation>) => {
      observers.push(observer);
      // Execute autorun lazily.
      if (handler === null) {
        handler = autorun();
      }
      return () => {
        removeObserver(observers,
          observer, () => handler.stop());
      };
    });
  }
}
