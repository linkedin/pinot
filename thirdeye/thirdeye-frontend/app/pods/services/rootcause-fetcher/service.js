import Service from '@ember/service';
import { get, getProperties, set, setProperties } from '@ember/object';
import fetch from 'fetch';
import _ from 'lodash';
import RSVP from 'rsvp';

/**
 * Comparator for of MyPromise for priority queue
 */
const ROOTCAUSE_FETCHER_COMPARATOR = (a, b) => {
  if (a.priority !== b.priority) { return a.priority < b.priority; }
  if (a.externalOrder !== b.externalOrder) { return a.externalOrder < b.externalOrder; }
  if (a.internalOrder !== b.internalOrder) { return a.internalOrder < b.internalOrder; }
  return true;
};

//
// FROM : https://stackoverflow.com/questions/42919469/efficient-way-to-implement-priority-queue-in-javascript
//
/**
 * Priority Queue via max-heap
 */
const top = 0;
const parent = i => ((i + 1) >>> 1) - 1;
const left = i => (i << 1) + 1;
const right = i => (i + 1) << 1;

class PriorityQueue {
  constructor(comparator = (a, b) => a > b) {
    this._heap = [];
    this._comparator = comparator;
  }

  size() {
    return this._heap.length;
  }

  isEmpty() {
    return this.size() == 0;
  }

  peek() {
    return this._heap[top];
  }

  push(...values) {
    values.forEach(value => {
      this._heap.push(value);
      this._siftUp();
    });
    return this.size();
  }

  pop() {
    const poppedValue = this.peek();
    const bottom = this.size() - 1;
    if (bottom > top) {
      this._swap(top, bottom);
    }
    this._heap.pop();
    this._siftDown();
    return poppedValue;
  }

  replace(value) {
    const replacedValue = this.peek();
    this._heap[top] = value;
    this._siftDown();
    return replacedValue;
  }

  _greater(i, j) {
    return this._comparator(this._heap[i], this._heap[j]);
  }

  _swap(i, j) {
    [this._heap[i], this._heap[j]] = [this._heap[j], this._heap[i]];
  }

  _siftUp() {
    let node = this.size() - 1;
    while (node > top && this._greater(node, parent(node))) {
      this._swap(node, parent(node));
      node = parent(node);
    }
  }

  _siftDown() {
    let node = top;
    while (
      (left(node) < this.size() && this._greater(left(node), node)) ||
      (right(node) < this.size() && this._greater(right(node), node))) {
      let maxChild = (right(node) < this.size() && this._greater(right(node), left(node))) ? right(node) : left(node);
      this._swap(node, maxChild);
      node = maxChild;
    }
  }
}

/**
 * Promise-lookalike for manual scheduling and execution via service
 */
class MyPromise {
  constructor (url, priority, externalOrder, internalOrder) {
    this.url = url;
    this.priority = priority;
    this.externalOrder = externalOrder;
    this.internalOrder = internalOrder;

    this.result = null;
    this.error = null;
    this.reject = null;
    this.resolve = null;

    this.delegate = new RSVP.Promise((resolve, reject) => {
      this.reject = reject;
      this.resolve = resolve;
    });
  }

  then(args) {
    return this.delegate.then(args);
  }

  catch(args) {
    return this.delegate.catch(args);
  }
}

/**
 * Query scheduler based on priority, external, and internal order. Supports configurable
 * make span.
 */
export default Service.extend({
  parallelism: 12,

  counter: 0,

  waiting: null,

  running: null,

  init() {
    this._super(...arguments);
    setProperties(this, { counter: 0, waiting: new PriorityQueue(ROOTCAUSE_FETCHER_COMPARATOR), running: [] });
  },

  fetch(url, priority = 0, externalOrder = 0) {
    const counter = get(this, 'counter');

    const promise = new MyPromise(url, priority, externalOrder, counter);

    set(this, 'counter', counter + 1);
    get(this, 'waiting').push(promise);

    this._update();

    return promise.delegate;
  },

  reset() {
    set(this, 'waiting', new PriorityQueue(ROOTCAUSE_FETCHER_COMPARATOR));
  },

  resetPrefix(urlPrefix) {
    const waiting = get(this, 'waiting');

    const newWaiting = new PriorityQueue(ROOTCAUSE_FETCHER_COMPARATOR);
    while (!waiting.isEmpty()) {
      if (!waiting.peek().url.startsWith(urlPrefix)) {
        newWaiting.push(waiting.pop());
      }
    }

    set(this, 'waiting', newWaiting);
  },

  _update() {
    const { waiting, running, parallelism } =
      getProperties(this, 'waiting', 'running', 'parallelism');

    const remaining = running.filter(p => p.result === null && p.error === null);

    const nStarting = parallelism - remaining.length;
    const starting = [];
    for (let i = 0; i < nStarting && !waiting.isEmpty(); i++) {
      starting.pushObject(waiting.pop()); // modified in-place
    }

    setProperties(this, { running: [...remaining, ...starting] });

    starting.forEach(p => {
      fetch(p.url)
        .then(res => { p.result = res; this._update(); })
        .catch(err => { p.error = err; this._update(); });
    });

    const done = running.filter(p => p.result !== null || p.error !== null);

    done.forEach(p => {
      if (p.result !== null) {
        p.resolve(p.result);
      }
      if (p.error !== null) {
        p.reject(p.error);
      }
      throw new Error(`Illegal state for fetcher promise ${p.id} (${p.url})`);
    });
  }
});
