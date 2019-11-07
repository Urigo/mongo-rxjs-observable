import { Subscriber } from 'rxjs';

import * as _ from 'lodash';

export declare type CallbacksObject = {
  onReady?: Function;
  onError?: Function;
  onStop?: Function;
};

export declare type MeteorCallbacks = ((...args) => any) | CallbacksObject;

export const subscribeEvents = ['onReady', 'onError', 'onStop'];

export function isMeteorCallbacks(callbacks: any): boolean {
  return _.isFunction(callbacks) || isCallbacksObject(callbacks);
}

// Checks if callbacks of {@link CallbacksObject} type.
export function isCallbacksObject(callbacks: any): boolean {
  return callbacks && subscribeEvents.some((event) => {
    return _.isFunction(callbacks[event]);
  });
}

declare const global;
export const g =
  typeof global === 'object' ? global :
    typeof window === 'object' ? window :
      typeof self === 'object' ? self : undefined;

const METEOR_RXJS_ZONE = 'meteor-rxjs-zone';

const fakeZone = {
  name: METEOR_RXJS_ZONE,
  run(func: Function) {
    return func();
  },
  fork(spec: any) {
    return fakeZone;
  }
};

export function forkZone() {
  if (g.Zone) {
    let zone = g.Zone.current;
    if (zone.name === METEOR_RXJS_ZONE) {
      zone = zone.parent || fakeZone;
    }
    return zone.fork({ name: METEOR_RXJS_ZONE });
  }
  return fakeZone;
}

export function getZone() {
  if (g.Zone) {
    let zone = g.Zone.current;
    if (zone.name === METEOR_RXJS_ZONE) {
      return zone.parent;
    }
    return zone;
  }
}

export function removeObserver(observers: Subscriber<any>[],
                               observer: Subscriber<any>,
                               onEmpty?: Function) {
  let index = observers.indexOf(observer);
  observers.splice(index, 1);
  if (observers.length === 0 && onEmpty) {
    onEmpty();
  }
}

export const gZone = g.Zone ? g.Zone.current : fakeZone;
