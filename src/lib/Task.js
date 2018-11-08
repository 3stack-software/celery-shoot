var
  debug = require('./debug')('celery:Task'),
  uuidv4 = require('uuid/v4'),
  async = require('async'),
  assert = require('assert'),
  hostname = require('os').hostname(),
  _ = require('lodash'),
  pid = process.pid,
  noop = function(){};


module.exports = (function () {
  /**
   * Create a task.
   *
   * Will choose exchange name with the following priority:
   *   1. taskOptions.exchangeName
   *   2. ROUTES{TaskName}.exchangeName
   *   3. Client.options.defaultExchange
   * Will choose the routing key with the following priority
   *   1. taskOptions.routingKey
   *   2. ROUTES{TaskName}.routingKey
   *   3. Client.options.defaultRoutingKey
   *
   * @param client
   * @param name
   * @param [defaultExecOptions] Passed as properties to the message
   *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
   *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
   * @param [taskOptions] Supports `ignoreResult` and `exchangeName` & `routingKey`
   *
   * @constructor
   */
  function Task(client, name, defaultExecOptions, taskOptions) {
    var self = this;

    self.client = client;
    self.name = name;
    self.defaultExecOptions = _.defaults(defaultExecOptions, {
      utc: true
    });
    self.taskOptions = _.defaults(taskOptions, {
      ignoreResult: false,
      exchangeName: null,
      routingKey: null
    });
    if (self.taskOptions.queueName != null){
      console.error('taskOptions.queueName is no longer supported. ' +
        'Please use taskOptions.queueName & taskOptions.routingKey instead.');
    }
  }

  /**
   *
   * Params Signature - ([args, [,kwargs[, execOptions]]][[,onStarted],onCompleted])
   * @param {Array} [args]
   * @param {Object} [kwargs]
   * @param {Object} [execOptions] Overwrite `defaultExecOptions` at runtime.
   * @param {Function} [onStarted]
   * @param {Function} [onCompleted]
   */
  Task.prototype.invoke = function CeleryTask_invoke(/* [args, [,kwargs [, execOptions]]][[,onStarted],onCompleted] */) {
    var self = this;
    var onCompleted = noop, onStarted = noop;

    var params = Array.prototype.slice.call(arguments, 0);

    assert(params.length <= 5);

    var lastParam = params.length > 0 && params[params.length - 1];
    if (lastParam && _.isFunction(lastParam)) {
      onCompleted = lastParam;
      params.pop();

      // or... the last param now..
      var secondLastParam = params.length > 0 && params[params.length - 1];
      if (secondLastParam && _.isFunction(secondLastParam)) {
        onStarted = secondLastParam;
        params.pop();
      }
    }

    var remainingParams = params.length;
    var args = remainingParams > 0 && params[0] || [];
    var kwargs = remainingParams > 1 && params[1] || {};
    var execOptions = remainingParams > 2 && params[2] || {};
    var options = _.defaults(execOptions, self.defaultExecOptions);
    var message = self.createMessage(args, kwargs, options);
    var route = self.client.lookupRoute(self.name, args, kwargs);
    // per call options will always overwrite lookups.
    if (self.taskOptions.exchange != null){
      route.exchange = self.taskOptions.exchange;
    }
    if (self.taskOptions.routingKey != null){
      route.routingKey = self.taskOptions.routingKey;
    }
    this.sendMessage(route.exchange, route.routingKey, message, onStarted, onCompleted);
  };

  function formatDateOrRelativeMs(date) {
    if (!(date instanceof Date)) {
      date = new Date(Date.now() + date);
    }
    return date.toISOString().slice(0, -1);
  }

  var ADDITIONAL_OPTIONS = ['retries', 'eta', 'expires', 'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timelimit'];
  Task.prototype.createMessage = function CeleryTask_createMessage(args, kwargs, options, id) {
    var self = this;
    var message = {
      id: id || uuidv4(),
      task: self.name,
      args: args,
      kwargs: kwargs,
      utc: true // dates should be considered UTC by default
    };
    // Are callers going to behave? & not send unsupported opts
    // var notAllowed = _.difference(ADDITIONAL_OPTIONS,_.keys(options));
    // if (notAllowed.length > 0) throw new Error("Unsupported options " + ",".join(notAllowed));
    _.extend(message, options);

    if (message.eta) {
      message.eta = formatDateOrRelativeMs(message.eta);
    }

    if (message.expires) {
      message.expires = formatDateOrRelativeMs(message.expires);
    }
    return message;
  };

  Task.prototype.createEvent = function CeleryTask_createEvent(exchange, routingKey, message) {
    var self = this;
    // Need to update for Celery 4.0 task_sent => before_task_publish
    //  http://docs.celeryproject.org/en/latest/internals/deprecation.html#removals-for-version-4-0
    //var event ={
    //  type: 'before_task_publish',
    //  body: message,
    //  exchange: exchange || 'celery',
    //  routing_key: routingKey || 'celery',
    //  headers: {},
    //  properties: {},
    //  declare: {},
    //  retry_policy: {}
    //};
    return _.extend(
      // Task/Message related data
      _.pick(message, 'args', 'kwargs', 'retries', 'expires', 'eta'), {
        uuid: message.id, // remap  id=>uuid
        name: message.task, // remap task=>name
        type: 'task-sent',

        // where did we send it?
        exchange: exchange,
        routing_key: routingKey,

        // identify the current host & time
        timestamp: (new Date()).getTime() / 1000,
        utcoffset: (new Date()).getTimezoneOffset() / -60,
        pid: pid,
        hostname: hostname
      })
  };

  /**
   * The following will happen in parallel:
   *   -> publishTask -> getResults(onStarted, onCompleted)
   *   -> publishSentEvent
   * @param {String} exchange
   * @param {String} routingKey
   * @param {Array} message
   * @param {Function} [onStarted]
   * @param {Function} [onCompleted]
   */
  Task.prototype.sendMessage = function CeleryTask_sendMessage(exchange, routingKey, message, onStarted, onCompleted) {
    var self = this;

    assert(self.client.connection.state === 'open');

    debug(5, 'publishTask');
    self.client.connection.publish(
      exchange, // exchange
      routingKey, // routing key
      message, //data
      {
        confirm: true,
        mandatory: false,
        immediate: false,
        contentEncoding: 'utf-8'
      },
      function(err, publishResponse){
        if (err != null){
          debug(1, ['publishTask failed!', message, err]);
          onCompleted(err);
        } else{
          debug(5, ['publishTask response', message, publishResponse]);
          if (!self.taskOptions.ignoreResult){
            if (Task.delayResults == null){
              self.getResult(message.id, onStarted, onCompleted);
            } else {
              setTimeout(function(){
                self.getResult(message.id, onStarted, onCompleted);
              }, Task.delayResults);
            }
          } else {
            onCompleted();
          }
        }
      }
    );

    if (self.client.options.sendTaskSentEvent) {
      debug(5, "sendTaskSent");
      var event = self.createEvent(exchange, routingKey, message);
      self.client.connection.publish(
        self.client.options.eventsExchange, // exchange
        'task.sent', // routing key
        event, // data
        {
          confirm: true,
          mandatory: false,
          immediate: false,
          contentEncoding: 'utf-8',
          headers: {
            hostname: hostname
          },
          deliveryMode: 2
        },
        function(err, publishResp){
          if (err != null) {
            debug(1, ['sendTaskSent failed!', err])
          } else {
            debug(5, ['sendTaskSent response', publishResp]);
          }
        }
      );
    }
  };

  /**
   * -> declare queue
   *   -> bind to exchange
   *   -> consume from queue
   *     -> consumeOk
   *     -> onMessage
   *       -> onStarted
   *       -> resolve
   * @param taskId
   * @param onStarted
   * @param onCompleted
   */
  Task.prototype.getResult = function CeleryTask_getResult(taskId, onStarted, onCompleted){
    var self = this;
    var resultsQueue = taskId.replace(/-/g, ''),
        resolved = false;

    // A special resolve function, as we could end up with multiple errors
    var resolve = function(err, result){
      if (resolved){
        debug(1, function(){ return ["Task result after already resolved.", resultsQueue, err]; });
        return;
      }
      resolved = true;
      if (err != null){
        debug(1, function(){ return ["An error occurred while resolving task", resultsQueue, err]; });
        onCompleted(err);
        return;
      }
      debug(5, function(){ return ["Task Resolved", resultsQueue, result]; });
      onCompleted(null, result);
    };

    debug(5, function(){ return ["Results - declareQueue", resultsQueue]});
    var queue = self.client.connection.queue(_.extend({
      queue: resultsQueue
    }, self.client.options.taskResultQueueOptions));

    queue.declare(function CeleryTask_declareQueue(queueDeclErr, queueDeclResp) {
      if (queueDeclErr != null) {
        debug(1, function () { return ["Results - declareQueueOk", resultsQueue, queueDeclErr] });
        resolve(queueDeclErr);
        return;
      }
      debug(5, function () { return ["Results - declareQueueOk", resultsQueue, queueDeclResp] });

      // Bit of a hack here,
      //  to distinguish between the `consumeOk` failing
      //  and failing to obtain a consumer all together.
      //  which the `amqp-coffee` does rather poorly.
      var mgr = self.client.connection.channelManager,
        errAndConsumer = mgr.consumerChannel(function(err, channelNo){
          // this function is called synchronously, and returns immediately
          if (err != null){
            return [err, null];
          }
          if (self.client.connection.channels[channelNo] != null){
            return [null, self.client.connection.channels[channelNo]];
          }
          return ["Tried to obtain a consumer, but the channelNo missing " + channelNo, null];
        }),
        obtainConsumerErr = errAndConsumer[0], consumerChannel = errAndConsumer[1];

      if (obtainConsumerErr != null){
        resolve(obtainConsumerErr);
        return;
      }

      async.parallel({
        resultsQueueBind: function CeleryTask_resultsQueueBind(done){
          debug(5, function(){ return ["queueBind", resultsQueue]});
          queue.bind(self.client.options.resultsExchange, '#', function(err, bindRes){
            if (err != null) {
              debug(1, function () { return ["queueBindOk - err", resultsQueue, err] });
              done(err)
            } else  {
              debug(5, function () { return ["queueBindOk", resultsQueue, bindRes] });
              done();
            }
          });
        },
        resultsConsume: function CeleryTask_resultsConsume(done) {
          debug(5, function(){ return ["consume", resultsQueue]});
          consumerChannel.consume(resultsQueue, {},
            function CeleryTask_onMessage(envelope) {
              var message = envelope.data,
                status = message.status.toLowerCase();

              debug(5, function(){ return ["consume - message", resultsQueue, status]});

              if (status === 'failure' || status === 'revoked' || status === 'ignored') {
                done(message);
              } else if (status === 'success') {
                // we fast track the result, so we don't have to wait
                // for the parallel "queueBindOk" and "closeConsumerCok" to complete.
                resolve(null, message);
                done();
              } else if (status === 'started' && onStarted) {
                onStarted(message);
              }
            },
            function CeleryTask_onConsumerOk(err, resp){
              // If we get an error from the consumeOk,
              // we won't receive any messages
              // so just abort immediately
              if (err){
                debug(1, function(){ return ["consumeOk", resultsQueue, err]});
                done(err);
              } else {
                debug(5, function(){ return ["consumeOk", resultsQueue, resp]});
              }
            }
          );
        }
      }, function(err){
        // By closing the consumer channel, the queue is deleted
        // so we need to wait for the queue-bind to finish first
        consumerChannel.close(function(err){
          if (err != null){
            debug(1, function(){ return ['Error from closing consumer.', resultsQueue]});
          }
        });
        if (err != null){
          debug(1, function(){ return ['resultsQueueBind/resultsConsume', err]; });
          resolve(err);
        } else if (!resolved){
          debug(1, function(){ return ['Not resolved...']; });
        }
      })
    });

  };

  return Task;
})();
