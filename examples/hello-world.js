const assert = require('assert');
const celery = require('../src/celery');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

var client = celery.connectWithUri(AMQP_HOST, function(err){
  assert(err == null);

  var task = client.createTask('tasks.echo');
  task.invoke(["Hello Wolrd"], function(err, result){
      console.log(err, result);
  })
});
