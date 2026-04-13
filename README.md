# Celery client for Node.js

Celery is an asynchronous task/job queue based on distributed
message passing. `celery-shoot` allows to queue tasks from Node.js.
If you are new to Celery check out http://celeryproject.org/

## Differences with `node-celery`

 1. This library is now based on [amqplib](https://github.com/squaremo/amqp.node),
    instead of [node-amqp](https://github.com/postwait/node-amqp).

 2. `EventEmitter` based code has been removed; only promises are available.

 3. Support for the Redis Backend has been removed.

    * I will accept pull-requests if you would like to re-add support.

 4. Primary Queue / Exchange declaration has been removed. This means if you start up
    `celery-shoot` on a fresh RabbitMQ vhost, you'll get an error.

    To get around this, just start a `celery worker` on that vhost first.

     * Why? If you declared your Queues/Exchanges from node, you need to mirror
       the celery settings _exactly_. If you don't, you need to stop both node &
       celery, delete the queues, restart both services (with the correct settings,
       or with the `celery worker` first). This was a big trap, that often came up
       deploying to production so we're better off without it!

## Usage

Simple example, included as [examples/hello-world.js](https://github.com/3stack-software/celery-shoot/blob/master/examples/hello-world.js):

```js
import { withClient } from 'celery-shoot';

withClient('amqp://guest:guest@localhost:5672//', {}, async client => {
  const result = await client.call({
    name: 'tasks.error',
    args: ['Hello World'],
  });
  console.log('tasks.echo response:', result);
});
```


### ETA

The ETA (estimated time of arrival) lets you set a specific date and time that is the earliest time at which your task will be executed:

```js
import { withClient } from 'celery-shoot';

withClient('amqp://guest:guest@localhost:5672//', {}, async client => {
  await client.call({
    name: 'tasks.send_email',
    kwargs: {
      to: 'to@example.com',
      title: 'sample email',
    },
    eta: 30 * 1000,
    ignoreResult: true,
  });
  console.log('tasks.send_email sent');
});
```

### Expiration

The expires argument defines an optional expiry time, a specific date and time using Date:

```javascript
import { withClient } from 'celery-shoot';

withClient('amqp://guest:guest@localhost:5672//', {}, async client => {
  await client.call({
    name: 'tasks.sleep',
    args: [2 * 60 * 60],
    expires: 1000, // in 1s
  });
});
```

### Routing

The simplest way to route tasks to different queues is using `options.routes`:

You can also configure custom routers, similar to http://celery.readthedocs.org/en/latest/userguide/routing.html#routers

```js
import { withClient } from 'celery-shoot';


const routes = [
  {
    'tasks.send_mail': {
      queue: 'mail',
    },
  },
  (task, args, kwargs) => {
    if(task === 'myapp.tasks.compress_video'){
      return {
        'exchange': 'video',
        'routingKey': 'video.compress'
      }
    }
    return null;
  }
];

withClient('amqp://guest:guest@localhost:5672//', { routes }, async client => {
  await client.call({
    name: 'tasks.send_email',
    kwargs: {
      to: 'to@example.com',
      title: 'sample email',
    },
    ignoreResult: true,
  });
  await client.call({
    name: 'tasks.calculate_rating',
    kwargs: {
      item: 1345,
    },
  });
});
```


```js
var myRouter = 
Client({
  routes: [myRouter]
});
```
