import Promise from 'bluebird';
import { BehaviorSubject } from 'rxjs';
import { first } from 'rxjs/operators';
import { connect as rawConnect } from './client';
import { debugError, debugLog } from './logging';

const INIT_BACKOFF = 1;
const MAX_BACKOFF = 120;

const DISCONNECTED = 0; // steady state
const DISCONNECTED_RETRY = 1; // has timer to attempt connection / reconnection
const CONNECTING = 2; // connection in progress
const CONNECTING_CLOSE = 3; // connection in progress, but close requested
const CONNECTED = 4; // connected
const CONNECTED_CLOSE = 6; // connected, but close requested

export class ReconnectingClient {
  constructor(connectionUri, options) {
    this.connectionUri = connectionUri;
    this.options = options;
    this.state = DISCONNECTED;
    this.state$ = new BehaviorSubject(DISCONNECTED);
    this._connectionAttempts = 0;
    this._backoffTimer = null;
    this._backoffDelay = 0;
    this._nextBackoffDelay = INIT_BACKOFF;
  }

  connect() {
    if (this.state === DISCONNECTED) {
      this.state = CONNECTING;
      this.state$.next(CONNECTING);
      this._backoffDelay = 0;
      this._nextBackoffDelay = INIT_BACKOFF;
      debugLog('connecting');
      this._connectionAttempts += 1;
      rawConnect(this.connectionUri, this.options).then(
        client => {
          this._connected(client);
        },
        err => {
          this._disconnected(err);
        },
      );
    } else if (this.state === DISCONNECTED_RETRY) {
      this.state = CONNECTING;
      this.state$.next(CONNECTING);
      debugLog(`connecting, attempt ${this._connectionAttempts}`);
      this._connectionAttempts += 1;
      rawConnect(this.connectionUri, this.options).then(
        client => {
          this._connected(client);
        },
        err => {
          this._disconnected(err);
        },
      );
    } else {
      debugError('Unexpected state, connect()', this.state);
    }
  }

  close() {
    if (this.state === CONNECTED) {
      this.state = CONNECTED_CLOSE;
      this.state$.next(CONNECTED_CLOSE);
      this.client.close().then(
        () => {
          this._disconnected();
        },
        err => {
          this._disconnected(err);
        },
      );
    } else if (this.state === CONNECTING) {
      this.state = CONNECTING_CLOSE;
      this.state$.next(CONNECTING_CLOSE);
    } else if (this.state === DISCONNECTED_RETRY) {
      this.state = DISCONNECTED;
      this.state$.next(DISCONNECTED);
      if (this._backoffTimer != null) {
        clearTimeout(this._backoffTimer);
        this._backoffTimer = null;
      }
    } else {
      debugError('Unexpected state, close()', this.state);
    }
  }

  _connected(client) {
    if (this.state === CONNECTING) {
      this.state = CONNECTED;
      this.state$.next(CONNECTED);
      this.client = client;
      this._backoffDelay = 0;
      this._nextBackoffDelay = INIT_BACKOFF;
      // client could close by itself
      client.whenClosed().then(() => {
        this._disconnected();
      });
    } else if (this.state === CONNECTING_CLOSE) {
      this.state = CONNECTED_CLOSE;
      this.state$.next(CONNECTED_CLOSE);
      this.client = client;
      client.close().then(
        () => {
          this._disconnected();
        },
        err => {
          this._disconnected(err);
        },
      );
    } else {
      debugError('Unexpected state, _connected()', this.state);
    }
  }

  _disconnected(err) {
    if (this.state === CONNECTED_CLOSE) {
      debugLog('disconnected', err);
      this.state = DISCONNECTED;
      this.state$.next(DISCONNECTED);
      this.client = null;
    } else if (this.state === CONNECTED || this.state === CONNECTING) {
      debugError(
        `unexpected disconnect, retrying in ${this._backoffDelay}s`,
        err,
      );
      this.state = DISCONNECTED_RETRY;
      this.state$.next(DISCONNECTED_RETRY);
      this._backoffTimer = setTimeout(() => {
        this.connect();
      }, this._backoffDelay * 1000);
      const delay = Math.min(this._nextBackoffDelay, MAX_BACKOFF);
      this._nextBackoffDelay += this._backoffDelay;
      this._backoffDelay = delay;
    } else if (this.state !== DISCONNECTED) {
      debugError('Unexpected state, _disconnected()', this.state, err);
    }
  }

  call(...args) {
    if (this.state !== CONNECTED) {
      throw new Error('Connection closed');
    }
    return this.client.call(...args);
  }

  async waitForDrain() {
    if (this.state !== CONNECTED) {
      throw new Error('Connection closed');
    }
    return this.client.waitForDrain();
  }

  async whenClosed() {
    return this.state$.pipe(first(state => state === DISCONNECTED)).toPromise();
  }

  async whenConnected() {
    return this.state$.pipe(first(state => state === CONNECTED)).toPromise();
  }
}

export async function connect(connectionUri, options) {
  const client = new ReconnectingClient(connectionUri, options);
  client.connect();
  await client.whenConnected();
  return client;
}

export function withClient(connectionUri, options, fn) {
  return Promise.using(
    Promise.resolve(
      connect(
        connectionUri,
        options,
      ),
    ).disposer(client => {
      client.close();
      return client.whenClosed();
    }),
    fn,
  );
}
