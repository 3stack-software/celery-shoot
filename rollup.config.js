import fs from 'node:fs';
import resolve from '@rollup/plugin-node-resolve';
import replace from '@rollup/plugin-replace';
import babel from '@rollup/plugin-babel';

const pkg = JSON.parse(
  fs.readFileSync(new URL('./package.json', import.meta.url)),
);

export default {
  input: 'src/index.js',
  external: [/node_modules/],
  output: [
    {
      file: pkg.exports.require,
      format: 'cjs',
      name: 'celery-shoot',
      sourcemap: false,
    },
    {
      file: pkg.exports.import,
      format: 'es',
      name: 'celery-shoot',
      sourcemap: false,
    },
  ],
  plugins: [
    replace({
      preventAssignment: true,
      values: {
        npm_package_version: pkg.version,
      },
    }),
    resolve(),
    babel({
      targets: {
        node: 'current',
      },
      presets: ['@babel/preset-env'],
      babelrc: false,
    }),
  ],
};
