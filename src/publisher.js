import defer from './defer';
import { asTaskV1, asTaskV2, serializeEvent, serializeTask } from './protocol';
import { debugLog, debugError } from './logging';

export class Publisher {
  constructor(channel) {
    this.channel = channel;
    this.closed = false;
    this.needsDrain = false;
    this.blocked = false;
    this.nextDrain = null;

    channel.addListener('error', this.handleChannelError);
    channel.addListener('close', this.handleChannelClose);
    channel.addListener('drain', this.handleDrain);
    channel.connection.addListener('blocked', this.handleConnectionBlocked);
    channel.connection.addListener('unblocked', this.handleConnectionUnblocked);
  }

  handleChannelError = err => {
    debugError('channel#publisher error', err);
    this.channel.connection.close();
  };

  handleChannelClose = () => {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.needsDrain = true;
    if (this.nextDrain != null) {
      // always be rejecting.
      this.nextDrain.reject(new Error('Connection closed'));
      this.nextDrain = null;
    }
    this.channel.removeListener('error', this.handleChannelError);
    this.channel.removeListener('close', this.handleChannelClose);
    this.channel.removeListener('drain', this.handleDrain);
    // clear props
    this.channel = null;
  };

  handleDrain = () => {
    this.needsDrain = false;
    if (this.nextDrain != null) {
      this.nextDrain.resolve();
      this.nextDrain = null;
    }
  };

  handleConnectionBlocked = () => {
    debugLog('server blocked publishing');
    this.blocked = true;
    if (this.nextDrain != null) {
      this.nextDrain.reject(new Error('connection blocked'));
      this.nextDrain = null;
    }
  };

  handleConnectionUnblocked = () => {
    debugLog('server unblocked publishing');
    this.blocked = false;
  };

  call({
    id,
    name,
    args = [],
    kwargs = {},
    exchange,
    routingKey,
    eventsExchange,
    taskProtocol,
    sendTaskSentEvent,
    ...options
  }) {
    let task;
    if (taskProtocol === 1) {
      task = asTaskV1(id, name, args, kwargs, options);
    } else {
      task = asTaskV2(id, name, args, kwargs, options);
    }
    const { headers, properties, body, sentEvent } = task;

    if (sendTaskSentEvent) {
      return this.publish(
        serializeTask(exchange, routingKey, headers, properties, body),
        serializeEvent(
          eventsExchange,
          'task-sent',
          sentEvent,
          exchange,
          routingKey,
        ),
      );
    }
    return this.publish(
      serializeTask(exchange, routingKey, headers, properties, body),
    );
  }

  async waitForDrain() {
    if (this.blocked) {
      throw new Error('blocked');
    }
    if (!this.needsDrain) {
      return null;
    }
    if (this.nextDrain == null) {
      this.nextDrain = defer();
    }
    return this.nextDrain.promise;
  }

  publish(...messages) {
    if (this.closed) {
      throw new Error('Connection Closed');
    }
    if (this.blocked) {
      throw new Error('Connection blocked by server.');
    }
    let writeResult = true;
    messages.forEach(({ exchange, routingKey, content, options }) => {
      writeResult = this.channel.publish(
        exchange,
        routingKey,
        content,
        options,
      );
    });
    this.needsDrain = !writeResult;
    return writeResult;
  }
}

export async function connect(connection) {
  const channel = await connection.createChannel();
  return new Publisher(channel);
}
