const assert = require('assert');
const each = require('async/each');
const celery = require('../src/celery');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

const n = parseInt(process.argv.length > 2 ? process.argv[2] : 100, 10);
const L = `tasksSent (n=${n})`;

function* series(a) {
  for(let i = 0; i < a; i += 1) {
    yield i;
  }
}

var client = celery.connectWithUri(AMQP_HOST, function (err) {
  assert(err == null);
  const task = client.createTask('tasks.add', {}, { ignoreResult: true });
  console.time(L);
  each(series(n), function (i, cb) {
    task.invoke([i, i], cb);
  }, function (err) {
    console.timeEnd(L);
    if (err) {
      console.error(err)
    }
    client.close(function() {
      console.log('done')
    });
  });
});
