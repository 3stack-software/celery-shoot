const Promise = require('bluebird');
const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

const n = parseInt(process.argv.length > 2 ? process.argv[2] : 100, 10);
const L = `tasksSent (n=${n})`;

withClient(
  AMQP_HOST,
  {
    sendTaskSentEvent: false,
  },
  async client => {
    console.time(L);
    for (let i = 0; i < n; i++) {
      try {
        const { writeResult } = client.call({
          name: 'tasks.add',
          args: [i, i],
          ignoreResult: true, // specifically ignore results for speed test
        });
        if (!writeResult) {
          console.log('waiting', i);
          await client.waitForDrain();
        }
      } catch (err) {
        console.log('error when i=', i, err);
        throw err;
      }
    }
    await client.waitForDrain();
    console.timeEnd(L);
  },
);
