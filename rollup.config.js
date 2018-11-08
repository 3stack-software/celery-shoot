import git from 'git-rev-sync';
import resolve from 'rollup-plugin-node-resolve';
import replace from 'rollup-plugin-replace';
import babel from 'rollup-plugin-babel';
import builtins from 'builtin-modules';
import pkg from './package.json';

const dependencies = Object.keys(pkg.dependencies);
export default {
  input: 'src/index.js',
  external(id) {
    // treat all our dependencies as external
    return (
      builtins.includes(id) ||
      dependencies.includes(id) ||
      dependencies.some(dep => id.startsWith(`${dep}/`))
    );
  },
  output: [
    {
      file: pkg.main,
      format: 'cjs',
      name: 'celery-shoot',
      sourcemap: false,
    },
    {
      file: pkg.module,
      format: 'es',
      name: 'celery-shoot',
      sourcemap: false,
    },
  ],
  plugins: [
    replace({
      npm_package_version: pkg.version,
      git_hash: git.short(),
    }),
    resolve(),
    babel({
      exclude: 'node_modules/**',
      presets: [
        [
          '@babel/env',
          {
            modules: false,
            targets: {
              node: true,
            },
          },
        ],
      ],
      plugins: ['@babel/plugin-proposal-class-properties'],
      babelrc: false,
    }),
  ],
};
