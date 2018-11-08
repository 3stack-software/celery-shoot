const assert = require('assert');
const celery = require('../src/celery');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

var client = celery.connectWithUri(AMQP_HOST, function(err){
  assert(err == null);

  var task = client.createTask('tasks.send_email', {
      eta: 60 * 60 * 1000 // execute in an hour from invocation
  }, {
      ignoreResult: true // ignore results
  });
  task.invoke([], {
    to: 'to@example.com',
    title: 'sample email'
  })
});
