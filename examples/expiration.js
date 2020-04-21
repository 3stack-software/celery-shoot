const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

withClient(AMQP_HOST, {}, async client => {
  await client.call({
    name: 'tasks.sleep',
    args: [2 * 60 * 60],
    expires: 1000, // in 1s
  });
});
