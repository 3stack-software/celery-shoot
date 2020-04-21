import amqp from 'amqplib';
import uuidv4 from 'uuid/v4';
import Promise from 'bluebird';
import { connect as connectBackend } from './backend';
import defer from './defer';
import { debugError, debugLog } from './logging';
import { connect as connectPublisher } from './publisher';

export class CeleryClient {
  constructor(connection, publisher, backend, options = {}) {
    this.connection = connection;
    this.publisher = publisher;
    this.backend = backend;
    this.options = {
      taskProtocol: 1,
      defaultExchange: 'celery',
      defaultRoutingKey: 'celery',
      eventsExchange: 'celeryev',
      sendTaskSentEvent: true,
      routes: {},
      ...options,
    };
    this.closed = false;
    this._whenClosed = defer();
    this.connection.on('error', this.handleConnectionError);
    this.connection.on('close', this.handleConnectionClose);
  }

  handleConnectionError = err => {
    debugError('connection error', err);
  };

  handleConnectionClose = () => {
    if (this.connection == null) {
      return;
    }
    this.closed = true;
    this._whenClosed.resolve();
    this.connection.removeListener('error', this.handleConnectionError);
    this.connection.removeListener('close', this.handleConnectionClose);

    this.connection = null;
    this._whenClosed = null;
  };

  async close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    debugLog('closing connection');
    await this.connection.close();
    debugLog('connection closed');
  }

  lookupRoute(taskName, args, kwargs) {
    const {
      routes,
      defaultExchange: exchange,
      defaultRoutingKey: routingKey,
    } = this.options;

    const routeArr = Array.isArray(routes) ? routes : [routes];
    for (const routerOrMap of routeArr) {
      let route;
      if (typeof routerOrMap === 'function') {
        // it's a router
        route = routerOrMap(taskName, args, kwargs);
      } else {
        // it's a map{taskName => route}
        route = routerOrMap[taskName];
      }
      if (route != null) {
        return {
          exchange,
          routingKey,
          ...route,
        };
      }
    }
    return { exchange, routingKey };
  }

  call({
    name,
    args = [],
    kwargs = {},
    exchange: overrideExchange = null,
    routingKey: overrideRoutingKey = null,
    ignoreResult = false,
    ...options
  }) {
    if (this.closed) {
      throw new Error('Connection closed');
    }
    const taskId = uuidv4();
    const { eventsExchange, taskProtocol, sendTaskSentEvent } = this.options;
    let { exchange, routingKey } = this.lookupRoute(name, args, kwargs);
    // per call options will always overwrite lookups.
    if (overrideExchange != null) {
      exchange = overrideExchange;
    }
    if (overrideRoutingKey != null) {
      routingKey = overrideRoutingKey;
    }

    let result = null;
    if (!ignoreResult) {
      // tell the backend to ack results referencing taskId
      result = this.backend.registerTask(taskId);
    }
    const writeResult = this.publisher.call({
      ...options,
      id: taskId,
      name,
      args,
      kwargs,
      exchange,
      routingKey,
      eventsExchange,
      taskProtocol,
      sendTaskSentEvent,
      replyTo: this.backend.queue,
    });
    return { writeResult, result };
  }

  async waitForDrain(timeout = 0) {
    if (this.closed) {
      throw new Error('Connection closed');
    }
    return this.publisher.waitForDrain(timeout);
  }

  async whenClosed() {
    if (this._whenClosed == null) {
      return;
    }
    return this._whenClosed.promise;
  }
}

export function connect(
  connectionUri,
  { socket: socketOptions = {}, backend: backendOptions = {}, ...options } = {},
) {
  const queue = uuidv4();
  return amqp
    .connect(
      connectionUri,
      socketOptions,
    )
    .then(connection =>
      Promise.all([
        connectPublisher(connection),
        connectBackend(connection, {
          ...backendOptions,
          queue,
        }),
      ]).spread(
        (publisher, backend) =>
          new CeleryClient(
            connection,
            publisher,
            backend, // TODO support rpc backend
            options,
          ),
      ),
    );
}

export function withClient(connectionUri, options, fn) {
  return Promise.using(
    connect(
      connectionUri,
      options,
    ).disposer(client => client.close()),
    fn,
  );
}
