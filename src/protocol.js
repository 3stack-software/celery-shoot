import os from 'os';

const HOSTNAME = os.hostname();
const PID = process.pid;

function formatDateOrRelativeMs(date) {
  if (!(date instanceof Date)) {
    date = new Date(Date.now() + date);
  }
  return date.toISOString().slice(0, -1);
}

function safeRepr(obj) {
  return JSON.stringify(obj).slice(0, 100);
}

export function asTaskV2(id, task, args, kwargs, options) {
  // adapted from: https://github.com/celery/celery/blob/master/celery/app/amqp.py as_task_v2
  // eslint-disable-next-line prefer-const
  let {
    eta = null,
    expires = null,
    groupId = null,
    retries = 0,
    chord = null,
    callbacks = null,
    errbacks = null,
    replyTo = '',
    timeLimit = null,
    softTimeLimit = null,
    rootId = null,
    parentId = null,
    shadow = null,
    chain = null,
    origin = null,
  } = options;
  if (eta != null) {
    eta = formatDateOrRelativeMs(eta);
  }
  if (expires != null) {
    expires = formatDateOrRelativeMs(expires);
  }

  const argsrepr = safeRepr(args);
  const kwargsrepr = safeRepr(kwargs);

  const headers = {
    lang: 'v8',
    task,
    id,
    shadow,
    eta,
    expires,
    group: groupId,
    retries,
    timelimit: [timeLimit, softTimeLimit],
    root_id: rootId,
    parent_id: parentId,
    argsrepr,
    kwargsrepr,
    origin: origin || HOSTNAME,
  };
  const properties = {
    correlationId: id,
    replyTo,
  };
  const body = [
    args,
    kwargs,
    {
      callbacks,
      errbacks,
      chain,
      chord,
    },
  ];
  const sentEvent = {
    uuid: id,
    root_id: rootId,
    parent_id: parentId,
    name: task,
    args: argsrepr,
    kwargs: kwargsrepr,
    retries,
    eta,
    expires,
  };
  return {
    headers,
    properties,
    body,
    sentEvent,
  };
}

export function asTaskV1(id, task, args, kwargs, options) {
  // adapted from: https://github.com/celery/celery/blob/master/celery/app/amqp.py as_task_v1
  let {
    eta = null,
    expires = null,
    groupId = null,
    retries = 0,
    chord = null,
    callbacks = null,
    errbacks = null,
    replyTo = '',
    timeLimit = null,
    softTimeLimit = null,
  } = options;
  if (eta != null) {
    eta = formatDateOrRelativeMs(eta);
  }
  if (expires != null) {
    expires = formatDateOrRelativeMs(expires);
  }

  const headers = {};
  const properties = {
    correlationId: id,
    replyTo,
  };
  const body = {
    task,
    id,
    args,
    kwargs,
    group: groupId,
    retries,
    eta,
    expires,
    utc: true,
    callbacks,
    errbacks,
    timelimit: [timeLimit, softTimeLimit],
    taskset: groupId,
    chord,
  };
  const sentEvent = {
    uuid: id,
    name: task,
    args: safeRepr(args),
    kwargs: safeRepr(kwargs),
    retries,
    eta,
    expires,
  };
  return {
    headers,
    properties,
    body,
    sentEvent,
  };
}

export function serializeTask(exchange, routingKey, headers, properties, body) {
  const content = Buffer.from(JSON.stringify(body));
  return {
    exchange,
    routingKey,
    content,
    options: {
      ...properties,
      // expiration: '100', // ms until message self-discards
      // userId: 'guest',  // must be same as connection
      // CC: otherRoutingKey,
      // BCC. otherRoutingKey,
      // priority: 1  // only applies to priority queue
      // mandatory: false, should be default?
      persistent: true,
      contentType: 'application/json',
      contentEncoding: 'utf-8',
      headers,
    },
  };
}

export function serializeEvent(
  exchange,
  type,
  event,
  taskExchange,
  taskRoutingKey,
) {
  const body = {
    ...event,
    exchange: taskExchange,
    routing_key: taskRoutingKey,
    pid: PID,
    hostname: HOSTNAME,
    utcoffset: new Date().getTimezoneOffset() / -60,
    timestamp: new Date().getTime() / 1000,
    clock: null,
  };
  const content = Buffer.from(JSON.stringify(body));
  return {
    exchange,
    routingKey: type.replace('-', '.'),
    content,
    options: {
      // expiration: '100', // ms until message self-discards
      // userId: 'guest',  // must be same as connection
      // CC: otherRoutingKey,
      // BCC. otherRoutingKey,
      // priority: 1  // only applies to priority queue
      // mandatory: false,
      persistent: true,
      contentType: 'application/json',
      contentEncoding: 'utf-8',
      headers: {
        hostname: HOSTNAME,
      },
    },
  };
}
