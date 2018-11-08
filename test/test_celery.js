/* eslint-env mocha */
const assert = require('assert');
const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

function getClient(fn) {
  return withClient(AMQP_HOST, {}, fn);
}

describe('celery functional tests', () => {
  describe('initialization', () => {
    it('should create a client without error', async () =>
      getClient(async client => {
        assert.ok(client != null);
      }));
  });

  describe('basic task calls', () => {
    it('should call a task without error', async () =>
      getClient(async client => {
        assert.ok(client != null);
        const result = await client.call({
          name: 'tasks.add',
          args: [1, 2],
        });
        assert.strictEqual(result, 3);
      }));
  });

  describe('eta', () => {
    it('should call a task with a delay', async () =>
      getClient(async client => {
        const calledAt = new Date().getTime();
        const delay = 1000;
        const acceptableDelay = 1000;

        const result = await client.call({
          name: 'tasks.curtime',
          eta: delay,
        });
        assert.ok(
          result > calledAt + acceptableDelay,
          `!(${result} > ${calledAt + acceptableDelay} )`,
        );
      }));
  });

  describe('expires', () => {
    it('should call a task which expires', async () =>
      getClient(async client => {
        const pastTime = -10 * 1000;

        try {
          await client.call({
            name: 'tasks.curtime',
            expires: pastTime,
          });
          assert.ok(false);
        } catch (err) {
          assert.strictEqual(err.status, 'REVOKED');
        }
      }));
  });
});
