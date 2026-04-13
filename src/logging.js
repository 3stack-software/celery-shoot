import Debug from 'debug';

const root = Debug('celery-shoot');

export const debugLog = root.extend('log');
export const debugError = root.extend('error');
