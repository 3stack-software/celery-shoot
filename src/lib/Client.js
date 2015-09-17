var
  debug = require('./debug')('celery:Client'),
  amqp = require('amqp-coffee'),
  Task = require('./Task'),
  mixinFuture = require('./Task.mixin.Futures'),
  URI = require('URIjs'),
  _ = require('underscore');


module.exports = (function() {
  /**
   * @param {amqp.Connection} connection
   * @param {Object} options
   * @param {String} [options.defaultQueue=celery]
   * @param {String} [options.defaultRoutingKey=null]
   * @param {String} [options.resultsExchange=celeryresults]
   * @param {String} [options.eventsExchange=celeryev]
   * @param {Boolean} [options.sendTaskSentEvent=true]
   * @param {Object} [options.taskResultQueueOptions] Settings for result queue (As per AMQP.Queue) {@link https://github.com/dropbox/amqp-coffee#connectionqueuequeueoptionscallback}
   * @param {Object} [options.routes] A mapping of Task Name => Route. Where Route = {queue: String, routingKey: String}
   * @constructor
   */
  function Client(connection, options) {
    var self = this;

    self.options = _.defaults(options, {
      defaultQueue: 'celery',
      defaultRoutingKey: null,
      resultsExchange: 'celeryresults',
      eventsExchange: 'celeryev',
      sendTaskSentEvent: true,
      taskResultQueueOptions: {
        autoDelete: true,
        noWait: false,
        exclusive: false,
        durable: true,
        passive: false,
        arguments: {
          "x-expires": 86400000 // 1 day
        }
      },
      routes: {}
    });
    self.connection = connection;
  }

  /**
   * Sig (connectionUri[[,options], clientConnected])
   * @param {Object|String} connectionUri eg 'amqp://guest:guest@localhost//'
   * @param {Object} [options]
   * @param {String} [options.defaultQueue=celery]
   * @param {String} [options.defaultRoutingKey=null]
   * @param {String} [options.resultsExchange=celeryresults]
   * @param {String} [options.eventsExchange=celeryev]
   * @param {Boolean} [options.sendTaskSentEvent=true]
   * @param {Object} [options.taskResultQueueOptions] Settings for result queue (As per AMQP.Queue) {@link https://github.com/dropbox/amqp-coffee#connectionqueuequeueoptionscallback}
   * @param {Object} [options.routes] A mapping of Task Name => Route. Where Route = {queue: String, routingKey: String}
   * @param {Function} clientConnected
   */
  Client.connectWithUri = function(connectionUri, options, clientConnected){
    if (_.isString(connectionUri)) {
      var uri = URI(connectionUri);
      connectionUri = {
        host: uri.hostname() || 'localhost',
        port: parseInt(uri.port(), 10) || 5672,
        login: uri.username() || 'guest',
        password: uri.password() || 'guest',
        vhost: uri.path().slice(1) || '/',
        ssl: uri.scheme() === 'amqps'
      }
    }
    if (_.isFunction(options)){
      clientConnected = options;
      options = {};
    }

    var connection = new amqp(_.defaults(connectionUri, {
      host: 'localhost',
      login: 'guest',
      password: 'guest',
      port: 5672,
      vhost: '/',
      heartbeat: 10000,
      reconnect: true,
      reconnectDelayTime: 1000
    }), clientConnected);

    return new Client(connection, options);
  };

  /**
   *
   * @param Future
   */
  Client.injectFuturesMixin = function(Future){
    mixinFuture(Future);
  };
  /**
   *
   * @param name
   * @param [defaultExecOptions] Passed as properties to the message
   *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
   *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
   * @param [taskOptions] Supports `ignoreResult`
   */
  Client.prototype.createTask = function Client_createTask(name, defaultExecOptions, taskOptions) {
    return new Task(this, name, defaultExecOptions || {}, taskOptions || {});
  };

  /**
   * Closes the connection!
   */
  Client.prototype.close = function Client_close(cb) {
    var self = this;
    self.connection.close();
    self.connection = null;
    _.defer(cb);
  };

  return Client;
})();