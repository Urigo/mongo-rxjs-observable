'use strict';
var __extends = (this && this.__extends) || (function () {
    var extendStatics = Object.setPrototypeOf ||
        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
        function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
import { Observable, Subject } from 'rxjs';
import { gZone, forkZone, removeObserver } from './utils';
var ObservableCursor = /** @class */ (function (_super) {
    __extends(ObservableCursor, _super);
    /**
     * @constructor
     * @extends Observable
     * @param {Mongo.Cursor<T>} cursor - The Mongo.Cursor to wrap.
     */
    function ObservableCursor(cursor) {
        var _this = _super.call(this, function (observer) {
            _this._observers.push(observer);
            if (!_this._hCursor) {
                _this._hCursor = _this._observeCursor(cursor);
            }
            var setTimeoutFn = Meteor.isClient ? setTimeout : Meteor.setTimeout;
            
            setTimeoutFn(function () {
                if (_this._isDataInitinialized) {
                    observer.next(_this._data);
                }
                else if (cursor.count() === 0) {
                    _this._isDataInitinialized = true;
                    observer.next(_this._data);
                }
            }, 0);
            return function () {
                removeObserver(_this._observers, observer, function () { return _this.stop(); });
            };
        }) || this;
        _this._data = [];
        _this._observers = [];
        _this._countObserver = new Subject();
        _this._isDataInitinialized = false;
        _.extend(_this, _.omit(cursor, 'count', 'map'));
        _this._cursor = cursor;
        _this._zone = forkZone();
        return _this;
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
    ObservableCursor.create = function (cursor) {
        return new ObservableCursor(cursor);
    };
    Object.defineProperty(ObservableCursor.prototype, "cursor", {
        /**
         * Returns the actual Mongo.Cursor that wrapped by current ObservableCursor instance.
         * @return {Mongo.Cursor<T>} The actual MongoDB Cursor.
         */
        get: function () {
            return this._cursor;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * A wrapper for Mongo.Cursor.count() method - returns an Observable of number, which
     * triggers each time there is a change in the collection, and exposes the number of
     * objects in the collection.
     * @returns {Observable} Observable which trigger the callback when the
     * count of the object changes.
     */
    ObservableCursor.prototype.collectionCount = function () {
        return this._countObserver.asObservable();
    };
    /**
     * Stops the observation on the cursor.
     */
    ObservableCursor.prototype.stop = function () {
        var _this = this;
        this._zone.run(function () {
            _this._runComplete();
        });
        if (this._hCursor) {
            this._hCursor.stop();
        }
        this._data = [];
        this._hCursor = null;
    };
    /**
     * Clears the Observable definition.
     * Use this method only when the Observable is still cold, and there are no active subscriptions yet.
     */
    ObservableCursor.prototype.dispose = function () {
        this._observers = null;
        this._cursor = null;
    };
    /**
     * Return all matching documents as an Array.
     *
     * @return {Array<T>} The array with the matching documents.
     */
    ObservableCursor.prototype.fetch = function () {
        return this._cursor.fetch();
    };
    /**
     * Watch a query. Receive callbacks as the result set changes.
     * @param {Mongo.ObserveCallbacks} callbacks - The callbacks object.
     * @return {Meteor.LiveQueryHandle} The array with the matching documents.
     */
    ObservableCursor.prototype.observe = function (callbacks) {
        return this._cursor.observe(callbacks);
    };
    /**
     * Watch a query. Receive callbacks as the result set changes.
     * Only the differences between the old and new documents are passed to the callbacks.
     * @param {Mongo.ObserveChangesCallbacks} callbacks - The callbacks object.
     * @return {Meteor.LiveQueryHandle} The array with the matching documents.
     */
    ObservableCursor.prototype.observeChanges = function (callbacks) {
        return this._cursor.observeChanges(callbacks);
    };
    ObservableCursor.prototype._runComplete = function () {
        this._countObserver.complete();
        this._observers.forEach(function (observer) {
            observer.complete();
        });
    };
    ObservableCursor.prototype._runNext = function (data) {
        this._countObserver.next(this._data.length);
        this._observers.forEach(function (observer) {
            observer.next(data);
        });
    };
    ObservableCursor.prototype._addedAt = function (doc, at, before) {
        this._data.splice(at, 0, doc);
        this._handleChange();
    };
    ObservableCursor.prototype._changedAt = function (doc, old, at) {
        this._data[at] = doc;
        this._handleChange();
    };
    ObservableCursor.prototype._removedAt = function (doc, at) {
        this._data.splice(at, 1);
        this._handleChange();
    };
    ObservableCursor.prototype._movedTo = function (doc, fromIndex, toIndex) {
        this._data.splice(fromIndex, 1);
        this._data.splice(toIndex, 0, doc);
        this._handleChange();
    };
    ObservableCursor.prototype._handleChange = function () {
        var _this = this;
        this._isDataInitinialized = true;
        this._zone.run(function () {
            _this._runNext(_this._data);
        });
    };
    ObservableCursor.prototype._observeCursor = function (cursor) {
        var _this = this;
        return gZone.run(function () { return cursor.observe({
            addedAt: _this._addedAt.bind(_this),
            changedAt: _this._changedAt.bind(_this),
            movedTo: _this._movedTo.bind(_this),
            removedAt: _this._removedAt.bind(_this)
        }); });
    };
    return ObservableCursor;
}(Observable));
export { ObservableCursor };
//# sourceMappingURL=ObservableCursor.js.map
