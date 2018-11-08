const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

withClient(AMQP_HOST, {}, async client => {
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
