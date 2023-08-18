import Promise from 'bluebird';

export default function defer() {
  let resolve;
  let reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return {
    resolve,
    reject,
    promise,
  };
}
