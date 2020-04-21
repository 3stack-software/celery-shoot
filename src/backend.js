import { ReplaySubject, throwError } from 'rxjs';
import { first } from 'rxjs/operators';
import defer from './defer';
import { debugError } from './logging';

export class CeleryResultError extends Error {
  constructor(status, result, traceback, ...params) {
    super(...params);
    this.status = status;
    this.result = result;
    this.traceback = traceback;
  }
}

class TaskResult {
  constructor(backend, taskId) {
    this.backend = backend;
    this.taskId = taskId;
  }

  destroy() {
    if (this.backend != null) {
      this.backend.destroyTask(this.taskId);
      this.backend = null;
      this.taskId = null;
    }
  }

  observe() {
    if (this.backend == null) {
      return throwError(new Error('destroyed'));
    }
    return this.backend.observeTask(this.taskId);
  }

  async whenStarted() {
    return this.waitForStatus('STARTED');
  }

  async waitForStatus(status = 'SUCCESS') {
    if (this.backend == null) {
      return new Error('destroyed');
    }
    return this.backend.waitForStatus(this.taskId, status);
  }

  async get() {
    try {
      const data = await this.waitForStatus();
      if (data == null) {
        throw new Error('Invalid result');
      }
      return data.result;
    } finally {
      this.destroy();
    }
  }
}

export default class Backend {
  constructor(channel, { queue = '', queueOptions }) {
    this.channel = channel;
    this.queue = queue;
    this.queueOptions = {
      exclusive: true,
      durable: true,
      autoDelete: true,
      passive: false,
      expires: 86400000,
      ...queueOptions,
    };
    this.closed = false;
    this.whenClosed = defer();
    this.sinks = new Map(); // correlationId => ResultStream
    channel.on('close', this.handleChannelClose);
    channel.on('error', this.handleChannelError);
  }

  handleChannelError = err => {
    debugError('channel#backend error', err);
    this.channel.connection.close();
  };

  handleChannelClose = () => {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.channel.removeListener('close', this.handleChannelClose);
    this.channel.removeListener('error', this.handleChannelError);
    this.sinks.forEach(sink => {
      if (!sink.isStopped) {
        sink.error(new Error('closed'));
      }
    });
    this.whenClosed.resolve();
    // clear props
    this.channel = null;
    this.queue = null;
    this.queueOptions = null;
    this.whenClosed = null;
    this.sinks = null;
  };

  async _consume() {
    await this.channel.assertQueue(this.queue, this.queueOptions);
    await this.channel.consume(this.queue, this.handleMessage);
  }

  handleMessage = msg => {
    if (this.closed) {
      return;
    }
    if (msg == null) {
      // broker cancelled the consumer
      this.handleChannelError();
    } else {
      this.dispatchMessage(msg);
    }
  };

  dispatchMessage(msg) {
    const { correlationId } = msg.properties;
    const sink = this.sinks.get(correlationId);
    if (sink == null) {
      this.channel.nack(msg);
    } else if (!sink.isStopped) {
      const data = JSON.parse(msg.content.toString());
      const s = data.status;
      if (s === 'FAILURE' || s === 'REVOKED' || s === 'IGNORED') {
        sink.error(
          new CeleryResultError(
            data.status,
            data.result,
            data.traceback,
            `Task ${correlationId} ${s}`,
          ),
        );
      } else {
        sink.next(data);
        if (s === 'SUCCESS') {
          sink.complete();
        }
      }
      this.channel.ack(msg);
    } else {
      this.channel.ack(msg);
    }
  }

  registerTask(taskId) {
    if (this.closed) {
      return new TaskResult(null, null);
    }
    if (this.sinks.has(taskId)) {
      return new TaskResult(this, taskId);
    }
    const sink = new ReplaySubject();
    this.sinks.set(taskId, sink);
    return new TaskResult(this, taskId);
  }

  destroyTask(taskId) {
    const sink = this.sinks.get(taskId);
    if (sink != null) {
      // new messages for this correlationId will go to dead letter queue
      this.sinks.delete(taskId);
      if (!sink.isStopped) {
        sink.error(new Error('destroyed'));
      }
    }
  }

  observeTask(taskId) {
    if (this.closed) {
      return throwError(new Error('closed'));
    }
    if (!this.sinks.has(taskId)) {
      return throwError(new Error('destroyed'));
    }
    return this.sinks.get(taskId);
  }

  async waitForStatus(taskId, status = 'SUCCESS') {
    return this.observeTask(taskId)
      .pipe(first(data => data.status === status))
      .toPromise();
  }
}

export async function connect(connection, options) {
  const channel = await connection.createChannel();
  const backend = new Backend(channel, options);
  await backend._consume();
  return backend;
}
