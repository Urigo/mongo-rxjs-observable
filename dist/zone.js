import { Subscriber } from 'rxjs';
import { getZone } from './utils';
export const zoneOperator = (zone) => (source) => source.lift(new ZoneOperator(zone || getZone()));
class ZoneOperator {
    constructor(zone) {
        this.zone = zone;
    }
    call(subscriber, source) {
        return source._subscribe(new ZoneSubscriber(subscriber, this.zone));
    }
}
class ZoneSubscriber extends Subscriber {
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
//# sourceMappingURL=zone.js.map