export const subscribeEvents = ['onReady', 'onError', 'onStop'];
export function isFunction(fn) {
    return typeof fn === 'function';
}
export function isMeteorCallbacks(callbacks) {
    return isFunction(callbacks) || isCallbacksObject(callbacks);
}
// Checks if callbacks of {@link CallbacksObject} type.
export function isCallbacksObject(callbacks) {
    return callbacks && subscribeEvents.some((event) => {
        return isFunction(callbacks[event]);
    });
}
export const g = typeof global === 'object' ? global :
    typeof window === 'object' ? window :
        typeof self === 'object' ? self : undefined;
const METEOR_RXJS_ZONE = 'meteor-rxjs-zone';
const fakeZone = {
    name: METEOR_RXJS_ZONE,
    run(func) {
        return func();
    },
    fork(spec) {
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
export function removeObserver(observers, observer, onEmpty) {
    let index = observers.indexOf(observer);
    observers.splice(index, 1);
    if (observers.length === 0 && onEmpty) {
        onEmpty();
    }
}
export const gZone = g.Zone ? g.Zone.current : fakeZone;
//# sourceMappingURL=utils.js.map