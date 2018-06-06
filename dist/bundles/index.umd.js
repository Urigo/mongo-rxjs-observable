(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('rxjs')) :
    typeof define === 'function' && define.amd ? define(['exports', 'rxjs'], factory) :
    (factory((global.meteor = global.meteor || {}, global.meteor.rxjs = {}),null));
}(this, (function (exports,rxjs) { 'use strict';

    const subscribeEvents = ['onReady', 'onError', 'onStop'];
    function isFunction(fn) {
        return typeof fn === 'function';
    }
    function isMeteorCallbacks(callbacks) {
        return isFunction(callbacks) || isCallbacksObject(callbacks);
    }
    // Checks if callbacks of {@link CallbacksObject} type.
    function isCallbacksObject(callbacks) {
        return callbacks && subscribeEvents.some((event) => {
            return isFunction(callbacks[event]);
        });
    }
    const g = typeof global === 'object' ? global :
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
    function forkZone() {
        if (g.Zone) {
            let zone = g.Zone.current;
            if (zone.name === METEOR_RXJS_ZONE) {
                zone = zone.parent || fakeZone;
            }
            return zone.fork({ name: METEOR_RXJS_ZONE });
        }
        return fakeZone;
    }
    function getZone() {
        if (g.Zone) {
            let zone = g.Zone.current;
            if (zone.name === METEOR_RXJS_ZONE) {
                return zone.parent;
            }
            return zone;
        }
    }
    function removeObserver(observers, observer, onEmpty) {
        let index = observers.indexOf(observer);
        observers.splice(index, 1);
        if (observers.length === 0 && onEmpty) {
            onEmpty();
        }
    }
    const gZone = g.Zone ? g.Zone.current : fakeZone;

    class ObservableCursor extends rxjs.Observable {
        /**
         * @constructor
         * @extends Observable
         * @param {Mongo.Cursor<T>} cursor - The Mongo.Cursor to wrap.
         */
        constructor(cursor) {
            super((observer) => {
                this._observers.push(observer);
                if (!this._hCursor) {
                    this._hCursor = this._observeCursor(cursor);
                }
                Meteor.setTimeout(() => {
                    if (this._isDataInitinialized) {
                        observer.next(this._data);
                    }
                    else if (cursor.count() === 0) {
                        this._isDataInitinialized = true;
                        observer.next(this._data);
                    }
                }, 0);
                return () => {
                    removeObserver(this._observers, observer, () => this.stop());
                };
            });
            this._data = [];
            this._observers = [];
            this._countObserver = new rxjs.Subject();
            this._isDataInitinialized = false;
            for (const key in cursor) {
                if (key !== 'count' && key !== 'map') {
                    this[key] = cursor[key];
                }
            }
            this._cursor = cursor;
            this._zone = forkZone();
        }
        /**
         *  Static method which creates an ObservableCursor from Mongo.Cursor.
         *  Use this to create an ObservableCursor object from an existing Mongo.Cursor.
         *  Prefer to create an Cursors from the ObservableCollection instance instead.
         *
         *  @param {Mongo.Cursor<T>} cursor - The Mongo.Cursor to wrap.
         *  @static
         *  @returns {ObservableCursor} Wrapped Cursor.
         */
        static create(cursor) {
            return new ObservableCursor(cursor);
        }
        /**
         * Returns the actual Mongo.Cursor that wrapped by current ObservableCursor instance.
         * @return {Mongo.Cursor<T>} The actual MongoDB Cursor.
         */
        get cursor() {
            return this._cursor;
        }
        /**
         * A wrapper for Mongo.Cursor.count() method - returns an Observable of number, which
         * triggers each time there is a change in the collection, and exposes the number of
         * objects in the collection.
         * @returns {Observable} Observable which trigger the callback when the
         * count of the object changes.
         */
        collectionCount() {
            return this._countObserver.asObservable();
        }
        /**
         * Stops the observation on the cursor.
         */
        stop() {
            this._zone.run(() => {
                this._runComplete();
            });
            if (this._hCursor) {
                this._hCursor.stop();
            }
            this._data = [];
            this._hCursor = null;
        }
        /**
         * Clears the Observable definition.
         * Use this method only when the Observable is still cold, and there are no active subscriptions yet.
         */
        dispose() {
            this._observers = null;
            this._cursor = null;
        }
        /**
         * Return all matching documents as an Array.
         *
         * @return {Array<T>} The array with the matching documents.
         */
        fetch() {
            return this._cursor.fetch();
        }
        /**
         * Watch a query. Receive callbacks as the result set changes.
         * @param {Mongo.ObserveCallbacks} callbacks - The callbacks object.
         * @return {Meteor.LiveQueryHandle} The array with the matching documents.
         */
        observe(callbacks) {
            return this._cursor.observe(callbacks);
        }
        /**
         * Watch a query. Receive callbacks as the result set changes.
         * Only the differences between the old and new documents are passed to the callbacks.
         * @param {Mongo.ObserveChangesCallbacks} callbacks - The callbacks object.
         * @return {Meteor.LiveQueryHandle} The array with the matching documents.
         */
        observeChanges(callbacks) {
            return this._cursor.observeChanges(callbacks);
        }
        _runComplete() {
            this._countObserver.complete();
            this._observers.forEach(observer => {
                observer.complete();
            });
        }
        _runNext(data) {
            this._countObserver.next(this._data.length);
            this._observers.forEach(observer => {
                observer.next(data);
            });
        }
        _addedAt(doc, at, before) {
            this._data.splice(at, 0, doc);
            this._handleChange();
        }
        _changedAt(doc, old, at) {
            this._data[at] = doc;
            this._handleChange();
        }
        _removedAt(doc, at) {
            this._data.splice(at, 1);
            this._handleChange();
        }
        _movedTo(doc, fromIndex, toIndex) {
            this._data.splice(fromIndex, 1);
            this._data.splice(toIndex, 0, doc);
            this._handleChange();
        }
        _handleChange() {
            this._isDataInitinialized = true;
            this._zone.run(() => {
                this._runNext(this._data);
            });
        }
        _observeCursor(cursor) {
            return gZone.run(() => cursor.observe({
                addedAt: this._addedAt.bind(this),
                changedAt: this._changedAt.bind(this),
                movedTo: this._movedTo.bind(this),
                removedAt: this._removedAt.bind(this)
            }));
        }
    }

    (function (MongoObservable) {
        /**
         *  Creates a new MongoObservable.Collection from an existing of predefined Mongo.Collection.
         *  Use this feature to wrap existing collections such as Meteor.users.
         *  @param {Mongo.Collection} collection - The collection.
         *  @returns {MongoObservable.Collection} - Wrapped collection.
         *  @static
         */
        function fromExisting(collection) {
            return new MongoObservable.Collection(collection);
        }
        MongoObservable.fromExisting = fromExisting;
        /**
         * A class represents a MongoDB collection in the client side, wrapped with RxJS
         * Observables, so you can use it with your Angular 2 easier.
         * The wrapper has the same API as Mongo.Collection, only the "find" method returns
         * an ObservableCursor instead of regular Mongo.Cursor.
         *
         * T is a generic type - should be used with the type of the objects inside the collection.
         */
        class Collection {
            /**
             *  Creates a new Mongo.Collection instance wrapped with Observable features.
             *  @param {String | Mongo.Collection} nameOrExisting - The name of the collection. If null, creates an
             *  unmanaged (unsynchronized) local collection. If provided an instance of existing collection, will
             *  create a wrapper for the existing Mongo.Collection.
             *  @param {ConstructorOptions} options - Creation options.
             *  @constructor
             */
            constructor(nameOrExisting, 
            // tslint:disable-next-line:align
            options) {
                if (nameOrExisting instanceof Mongo.Collection) {
                    this._collection = nameOrExisting;
                }
                else {
                    this._collection = new Mongo.Collection(nameOrExisting, options);
                }
            }
            /**
             *  Returns the Mongo.Collection object that wrapped with the MongoObservable.Collection.
             *  @returns {Mongo.Collection<T>} The Collection instance
             */
            get collection() {
                return this._collection;
            }
            /**
             *  Allow users to write directly to this collection from client code, subject to limitations you define.
             *
             *  @returns {Boolean}
             */
            allow(options) {
                return this._collection.allow(options);
            }
            /**
             *  Override allow rules.
             *
             *  @returns {Boolean}
             */
            deny(options) {
                return this._collection.deny(options);
            }
            /**
             *  Returns the Collection object corresponding to this collection from the npm
             *  mongodb driver module which is wrapped by Mongo.Collection.
             *
             *  @returns {Mongo.Collection} The Collection instance
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-rawCollection|rawCollection on Meteor documentation}
             */
            rawCollection() {
                return this._collection.rawCollection();
            }
            /**
             *  Returns the Db object corresponding to this collection's database connection from the
             *  npm mongodb driver module which is wrapped by Mongo.Collection.
             *
             *  @returns {Mongo.Db} The Db instance
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-rawDatabase|rawDatabase on Meteor documentation}
             */
            rawDatabase() {
                return this._collection.rawDatabase();
            }
            /**
             *  Insert a document in the collection.
             *
             *  @param {T} doc - The document to insert. May not yet have an _id
             *  attribute, in which case Meteor will generate one for you.
             *  @returns {Observable<string>} Observable which completes with the inserted ObjectId
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-insert|insert on Meteor documentation}
             */
            insert(doc) {
                let observers = [];
                let obs = this._createObservable(observers);
                this._collection.insert(doc, (error, docId) => {
                    observers.forEach(observer => {
                        error ? observer.error(error) :
                            observer.next(docId);
                        observer.complete();
                    });
                });
                return obs;
            }
            /**
             *  Remove documents from the collection.
             *
             *  @param {Collection~MongoQueryMongo.Selector} selector - Specifies which documents to modify
             *  @returns {Observable<Number>} Observable which completes with the number of affected rows
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-remove|remove on Meteor documentation}
             */
            remove(selector) {
                let observers = [];
                let obs = this._createObservable(observers);
                this._collection.remove(selector, (error, removed) => {
                    observers.forEach(observer => {
                        error ? observer.error(error) :
                            observer.next(removed);
                        observer.complete();
                    });
                });
                return obs;
            }
            /**
             *  Modify one or more documents in the collection.
             *
             *  @param {Collection~MongoQueryMongo.Selector} selector - Specifies which documents to modify
             *  @param {Modifier} modifier - Specifies how to modify the documents
             *  @param {MongoUpdateOptions} options - Update options
             *  first argument and, if no error, the number of affected documents as the second
             *  @returns {Observable<Number>} Observable which completes with the number of affected rows
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-update|update on Meteor documentation}
             */
            update(selector, 
            // tslint:disable-next-line:align
            modifier, 
            // tslint:disable-next-line:align
            options) {
                let observers = [];
                let obs = this._createObservable(observers);
                this._collection.update(selector, modifier, options, (error, updated) => {
                    observers.forEach(observer => {
                        error ? observer.error(error) :
                            observer.next(updated);
                        observer.complete();
                    });
                });
                return obs;
            }
            /**
             *  Finds the first document that matches the selector, as ordered by sort and skip options.
             *
             *  @param {Collection~MongoQueryMongo.Selector} selector - Specifies which documents to modify
             *  @param {Modifier} modifier - Specifies how to modify the documents
             *  @param {MongoUpsertOptions} options - Upsert options
             *  first argument and, if no error, the number of affected documents as the second.
             *  @returns {Observable<{numberAffected, insertedId}>} Observable which completes with an
             *  Object that contain the keys numberAffected and insertedId.
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-upsert|upsert on Meteor documentation}
             */
            upsert(selector, 
            // tslint:disable-next-line:align
            modifier, 
            // tslint:disable-next-line:align
            options) {
                let observers = [];
                let obs = this._createObservable(observers);
                this._collection.upsert(selector, modifier, options, (error, affected) => {
                    observers.forEach(observer => {
                        error ? observer.error(error) :
                            observer.next(affected);
                        observer.complete();
                    });
                });
                return obs;
            }
            /**
             *  Method has the same notation as Mongo.Collection.find, only returns Observable.
             *
             *  @param {Collection~MongoQueryMongo.Selector} selector - A query describing the documents to find
             *  @param {Collection~MongoQueryOptions} options - Query options, such as sort, limit, etc.
             *  @returns {ObservableCursor<T>} RxJS Observable wrapped with Meteor features.
             *  @example <caption>Using Angular2 Component</caption>
             *  const MyCollection = MongoObservable.Collection("myCollection");
             *
             *  class MyComponent  {
             *     private myData: ObservableCursor<any>;
             *
             *     constructor() {
             *        this.myData = MyCollection.find({}, {limit: 10});
             *     }
             *  }
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-find|find on Meteor documentation}
             */
            find(selector, options) {
                const cursor = this._collection.find.apply(this._collection, arguments);
                return ObservableCursor.create(cursor);
            }
            /**
             *  Finds the first document that matches the selector, as ordered by sort and skip options.
             *
             *  @param {Collection~MongoQueryMongo.Selector} selector - A query describing the documents to find
             *  @param {Collection~MongoQueryOptions} options - Query options, such as sort, limit, etc.
             *  @returns {any} The first object, or `undefined` in case of non-existing object.
             *
             * @see {@link https://docs.meteor.com/api/collections.html#Mongo-Collection-findOne|findOne on Meteor documentation}
             */
            findOne(selector, options) {
                return this._collection.findOne.apply(this._collection, arguments);
            }
            _createObservable(observers) {
                return rxjs.Observable.create((observer) => {
                    observers.push(observer);
                    return () => {
                        removeObserver(observers, observer);
                    };
                });
            }
        }
        MongoObservable.Collection = Collection;
    })(exports.MongoObservable || (exports.MongoObservable = {}));
    /**
     * An options object for MongoDB queries.
     * @typedef {Object} Collection~MongoQueryOptions
     * @property {Object} sort - Sort order (default: natural order)
     * @property {Number} skip - Number of results to skip at the beginning
     * @property {Object} fields - Dictionary of fields to return or exclude.
     * @property {Boolean} reactive - (Client only) Default true; pass false to disable reactivity
     * @property {Function} transform - Overrides transform on the Collection for this cursor. Pass null to disable transformation.
     */
    /**
     * A MongoDB query selector representation.
     * @typedef {(Mongo.Mongo.Selector|Mongo.Mongo.ObjectID|string)} Collection~MongoQueryMongo.Selector
     */
    /**
     * A MongoDB query options for upsert action
     * @typedef {Object} Collection~MongoUpsertOptions
     * @property {Boolean} multi - True to modify all matching documents;
     * false to only modify one of the matching documents (the default).
     */
    /**
     * A MongoDB query options for update action
     * @typedef {Object} Collection~MongoUpdateOptions
     * @property {Boolean} multi - True to modify all matching documents;
     * @property {Boolean} upsert - True to use upsert logic.
     */

    let liveSubscriptions = [];
    function throwInvalidCallback(method) {
        throw new Error(`Invalid ${method} arguments:
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
    class MeteorObservable {
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
        static call(name, ...args) {
            const lastParam = args[args.length - 1];
            if (isMeteorCallbacks(lastParam)) {
                throwInvalidCallback('MeteorObservable.call');
            }
            let zone = forkZone();
            return rxjs.Observable.create((observer) => {
                Meteor.call(name, ...args.concat([
                    (error, result) => {
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
        static subscribe(name, ...args) {
            let lastParam = args[args.length - 1];
            if (isMeteorCallbacks(lastParam)) {
                throwInvalidCallback('MeteorObservable.subscribe');
            }
            let zone = forkZone();
            let observers = [];
            let subscribe = () => {
                return Meteor.subscribe(name, ...args.concat([{
                        onError: (error) => {
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
            return rxjs.Observable.create((observer) => {
                observers.push(observer);
                // Execute subscribe lazily.
                if (subHandler === null) {
                    subHandler = subscribe();
                    if (liveSubscriptions.find(sub => sub === subHandler.subscriptionId)) {
                        // subscription already exists, call observer.next() since Meteor won't.
                        observer.next();
                    }
                    else {
                        liveSubscriptions.push(subHandler.subscriptionId);
                    }
                }
                return () => {
                    removeObserver(observers, observer, () => {
                        // remove subscription from liveSubscriptions list
                        let i = liveSubscriptions.findIndex(sub => sub === subHandler.subscriptionId);
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
        static autorun() {
            let zone = forkZone();
            let observers = [];
            let autorun = () => {
                return Tracker.autorun((computation) => {
                    zone.run(() => {
                        observers.forEach(observer => observer.next(computation));
                    });
                });
            };
            let handler = null;
            return rxjs.Observable.create((observer) => {
                observers.push(observer);
                // Execute autorun lazily.
                if (handler === null) {
                    handler = autorun();
                }
                return () => {
                    removeObserver(observers, observer, () => handler.stop());
                };
            });
        }
    }

    const zoneOperator = (zone) => (source) => source.lift(new ZoneOperator(zone || getZone()));
    class ZoneOperator {
        constructor(zone) {
            this.zone = zone;
        }
        call(subscriber, source) {
            return source._subscribe(new ZoneSubscriber(subscriber, this.zone));
        }
    }
    class ZoneSubscriber extends rxjs.Subscriber {
        constructor(destination, zone) {
            super(destination);
            this.zone = zone;
        }
        _next(value) {
            this.zone.run(() => {
                this.destination.next(value);
            });
        }
        _complete() {
            this.zone.run(() => {
                this.destination.complete();
            });
        }
        _error(err) {
            this.zone.run(() => {
                this.destination.error(err);
            });
        }
    }

    exports.MeteorObservable = MeteorObservable;
    exports.ObservableCursor = ObservableCursor;
    exports.zoneOperator = zoneOperator;

    Object.defineProperty(exports, '__esModule', { value: true });

})));
