/* eslint-env mocha */
import assert from 'node:assert';
import { withClient } from '../dist/celery-shoot.esm.js';

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

function getClient(fn) {
  return withClient(AMQP_HOST, {}, fn);
}

describe('celery functional tests', () => {
  describe('initialization', () => {
    it('should create a client without error', async () =>
      getClient(async (client) => {
        assert.ok(client != null);
      }));
  });

  describe('basic task calls', () => {
    it('should call a task without error', async () =>
      getClient(async (client) => {
        assert.ok(client != null);
        const result = await client
          .call({
            name: 'tasks.add',
            args: [1, 2],
          })
          .result.get();
        assert.strictEqual(result, 3);
      }));
  });

  describe('eta', () => {
    it('should call a task with a delay', async () =>
      getClient(async (client) => {
        const calledAt = Date.now();
        const delay = 1500;
        const acceptableDelay = 1000;

        const result = await client
          .call({
            name: 'tasks.curtime',
            eta: delay,
          })
          .result.get();
        const resultAt = Date.now();
        assert.ok(
          resultAt - calledAt > delay,
          `delay should be minimum ${delay}, got ${resultAt - calledAt}`,
        );
        assert.ok(
          result > calledAt + acceptableDelay,
          `!(${result} > ${calledAt + acceptableDelay} )`,
        );
      }));
  }).timeout(4000);

  describe('expires', () => {
    it('should call a task which expires', async () =>
      getClient(async (client) => {
        const pastTime = -10 * 1000;

        try {
          await client
            .call({
              name: 'tasks.curtime',
              expires: pastTime,
            })
            .result.get();
          assert.ok(false, 'unreachable');
        } catch (err) {
          assert.strictEqual(err.status, 'REVOKED');
        }
      }));
  });
});
