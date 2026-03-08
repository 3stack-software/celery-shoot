/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable max-classes-per-file */

// warning: shamelessly vibe-coded

declare module 'celery-shoot' {
  import type { Observable } from 'rxjs';

  /**
   * The version of the celery-shoot package, composed of the npm package version and git hash.
   */
  export const version: string;

  /**
   * Options for a Celery task call.
   */
  export interface CallOptions {
    /** The name of the task to invoke, e.g., "tasks.send_email". */
    name: string;
    /** Positional arguments for the task. */
    args?: any[];
    /** Keyword arguments for the task. */
    kwargs?: Record<string, any>;
    /** Override the exchange to publish the task to. */
    exchange?: string;
    /** Override the routing key (queue) to publish the task to. */
    routingKey?: string;
    /** If true, do not track or wait for a result. */
    ignoreResult?: boolean;
    /** Earliest time the task should be executed; number of milliseconds or Date. */
    eta?: number | Date;
    /** Expiration time for the task; number of milliseconds or Date. */
    expires?: number | Date;
    /** Priority for the task, if the broker supports priority queues. */
    priority?: number;
    /** Additional protocol or header options (e.g., callbacks, errbacks). */
    [key: string]: any;
  }

  /**
   * The result of invoking a Celery task through CeleryClient.call().
   */
  export interface CallResult {
    /** The boolean return value of the underlying RabbitMQ publish call (true if the write did not exceed high water mark). */
    writeResult: boolean;
    /** A TaskResult to observe or await the task’s result, or null if ignoreResult was true. */
    result: TaskResult | null;
  }

  /**
   * Represents a Celery task’s asynchronous result. Allows observing status and retrieving the final result.
   */
  export class TaskResult {
    /**
     * Destroy the result subscription and release resources.
     */
    destroy(): void;

    /**
     * Return an RxJS Observable that emits status events for the task.
     */
    observe(): Observable<any>;

    /**
     * Await until the task status becomes "STARTED".
     */
    whenStarted(): Promise<any>;

    /**
     * Await until the task status matches the given status (default: "SUCCESS").
     * @param status The status to wait for (e.g., "FAILURE", "SUCCESS").
     */
    waitForStatus(status?: string): Promise<any>;

    /**
     * Await and retrieve the final result of the task, then automatically destroy resources.
     */
    get(): Promise<any>;
  }

  /**
   * Error thrown when a Celery task results in a failure. Contains status, result, and traceback.
   */
  export class CeleryResultError extends Error {
    status: string;

    result: any;

    traceback: any;

    constructor(status: string, result: any, traceback: any, ...params: any[]);
  }

  interface Route {
    queue?: string;
    exchange?: string;
    routingKey?: string;
  }

  /**
   * Options to configure a CeleryClient instance.
   */
  export interface ClientOptions {
    /** Celery protocol version; currently 1 or 2. */
    taskProtocol?: number;
    /** Default AMQP exchange for task messages (default: "celery"). */
    defaultExchange?: string;
    /** Default AMQP routing key (queue) for task messages (default: "celery"). */
    defaultRoutingKey?: string;
    /** Exchange for task-related events (e.g., task-sent). */
    eventsExchange?: string;
    /** If true, publish a "task-sent" event when sending a task. */
    sendTaskSentEvent?: boolean;
    /**
     * Routing configuration. Can be a single mapping of taskName to route, an array of mappings and/or router functions,
     * or a function that determines per-task routing.
     */
    routes?:
      | Array<
          | Record<string, Route>
          | ((taskName: string, args: any[], kwargs: any) => Route | null)
        >
      | Record<string, Route>;
    /** Additional arbitrary options for internal publishers or backends. */
    [key: string]: any;
  }

  /**
   * A client that automatically reconnects to RabbitMQ on failures and wraps a CeleryClient internally.
   */
  class ReconnectingClient {
    /** The raw state number (0: DISCONNECTED, 1: DISCONNECTED_RETRY, 2: CONNECTING, 3: CONNECTING_CLOSE, 4: CONNECTED, 6: CONNECTED_CLOSE). */
    state: number;

    /** Observable emitting state changes. */
    state$: Observable<number>;

    constructor(
      /** AMQP connection URI, e.g., "amqp://guest:guest@localhost:5672//". */
      connectionUri: string,
      /** Connection options (socket options, backend options). */
      options?: ClientOptions,
    );

    /**
     * Initiate or retry connection to the broker.
     */
    connect(): void;

    /**
     * Signal that the client should close the connection once connected or immediately if already connected.
     */
    close(): void;

    /**
     * Proxy to CeleryClient.call; sends a task. Throws if not connected.
     */
    call(options: CallOptions): CallResult;

    /**
     * Proxy to CeleryClient.waitForDrain (ensures published tasks have been sent). Throws if not connected.
     * @param timeout Timeout in milliseconds.
     */
    waitForDrain(timeout?: number): Promise<any>;

    /**
     * Returns a promise that resolves when the client has disconnected.
     */
    whenClosed(): Promise<void>;

    /**
     * Returns a promise that resolves when the client has connected.
     */
    whenConnected(): Promise<void>;
  }

  export { ReconnectingClient as Client };

  /**
   * Establish a connection to the broker, returning a ReconnectingClient once connected.
   * @param connectionUri The AMQP URI string.
   * @param options Optional settings, such as socket and backend options.
   */
  export function connect(
    connectionUri: string,
    options?: ClientOptions,
  ): Promise<ReconnectingClient>;

  /**
   * Convenience helper that connects to the broker, invokes a user-supplied function, then closes the connection.
   * @param connectionUri The AMQP URI string.
   * @param options ClientOptions for task routing and protocol versions.
   * @param fn Async function called with the connected ReconnectingClient.
   */
  export function withClient(
    connectionUri: string,
    options: any,
    fn: (client: ReconnectingClient) => Promise<any>,
  ): Promise<void>;
}
