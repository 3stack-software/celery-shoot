const Promise = require('bluebird');
const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

const n = parseInt(process.argv.length > 2 ? process.argv[2] : 100, 10);
const L = `tasksCompleted (n=${n})`;

withClient(
  AMQP_HOST,
  {
    sendTaskSentEvent: false,
  },
  async client => {
    console.time(L);
    const promises = [];
    for (let i = 0; i < n; i++) {
      const { writeResult, result } = client.call({
        name: 'tasks.add',
        args: [i, i],
      });
      if (!writeResult) {
        console.log('waiting', i);
        await client.waitForDrain();
      }
      promises.push(result.get());

      if (promises.length >= 1000) {
        console.log('getting results', i, promises.length);
        await Promise.all(promises);
        promises.length = 0;
      }
    }
    console.log('all tasks sent');
    return Promise.all(promises).then(
      () => {
        console.timeEnd(L);
      },
      err => {
        console.timeEnd(L);
        console.error(err);
      },
    );
  },
);
