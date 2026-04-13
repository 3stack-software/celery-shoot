import { withClient } from '../dist/celery-shoot.esm.js';

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

withClient(AMQP_HOST, {}, async (client) => {
  const N = 20;
  const SLEEP = 1;
  const MAX_PRIORITY = 2;

  const all = [];
  // publish low -> high priority, but expect completions from high -> low
  for (let priority = 0; priority <= MAX_PRIORITY; priority += 1) {
    for (let i = 0; i < N; i += 1) {
      const { result } = client.call({
        name: 'tasks.sleep_and_echo',
        args: [SLEEP, `priority=${priority}`],
        priority,
      });
      all.push(
        result.get().then((msg) => {
          console.log(msg);
        }),
      );
    }
  }
  await Promise.all(all);
});
