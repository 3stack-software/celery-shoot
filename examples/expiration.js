const assert = require('assert');
const celery = require('../src/celery');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

var client = celery.connectWithUri(AMQP_HOST, function(err){
  assert(err == null);

  var task = client.createTask('tasks.sleep', {
      eta: 60 * 60 * 1000 // expire in an hour
  });
  task.invoke([2 * 60 * 60], function(err, res){
      console.log(err, res);
  })
});
