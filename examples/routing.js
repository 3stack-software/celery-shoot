const assert = require('assert');
const celery = require('../src/celery');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

var client = celery.connectWithUri(AMQP_HOST, {
  routes: {
      'tasks.send_mail': {
          'queue': 'mail'
      }
  }
}, function(err){
  assert(err == null);

  var task = client.createTask('tasks.send_email');
  task.invoke([], {
    to: 'to@example.com',
    title: 'sample email'
  });
  var task2 = client.createTask('tasks.calculate_rating');
  task2.invoke([], {
      item: 1345
  });
});
