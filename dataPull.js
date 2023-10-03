const cron = require('node-cron');

const { cxnInit } = require('./cxn.service');
const { skInit } = require('./sk.service');

const { writeFile } = require('fs');
const util = require('node:util');

const promiseWriteFile = util.promisify(writeFile);

const cxnUsers = [
  {
    email: 'rubi@aka-extensions.com',
    password: 'sdfLKNl34$jhndJ',
  },
  {
    email: 'sean@aka-extensions.com',
    password: 'fsdfJKHjknbkl235$',
  },
];
const fetchData = async () => {
  try {
    const skResult = await skInit();
    const cxnResult = await cxnInit({ users: cxnUsers });

    const toWrite =
      JSON.stringify({
        skResult,
        cxnResult,
        dateNow: new Date().toISOString(),
      }) + '\n';

    await promiseWriteFile('logs.txt', toWrite, { flag: 'a' });

    console.log('--DONE--');
  } catch (error) {
    console.error('Error:', error.message);
  }
};

console.log('Scheduler started.');

cron.schedule('0 0 * * *', () => {
  console.log('Running data pull...');
  fetchData();
});
