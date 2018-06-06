import { Observable, Subject } from 'rxjs';
import { gZone, forkZone, removeObserver } from './utils';
export class ObservableCursor extends Observable {
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
        this._countObserver = new Subject();
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
//# sourceMappingURL=ObservableCursor.js.map