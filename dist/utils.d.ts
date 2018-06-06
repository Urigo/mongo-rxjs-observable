import { Subscriber } from 'rxjs';
export declare type CallbacksObject = {
    onReady?: Function;
    onError?: Function;
    onStop?: Function;
};
export declare type MeteorCallbacks = ((...args) => any) | CallbacksObject;
export declare const subscribeEvents: string[];
export declare function isFunction(fn: any): boolean;
export declare function isMeteorCallbacks(callbacks: any): boolean;
export declare function isCallbacksObject(callbacks: any): boolean;
export declare const g: any;
export declare function forkZone(): any;
export declare function getZone(): any;
export declare function removeObserver(observers: Subscriber<any>[], observer: Subscriber<any>, onEmpty?: Function): void;
export declare const gZone: any;
