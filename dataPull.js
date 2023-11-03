const cron = require('node-cron');

const { cxnInit } = require('./cxn.service');
const { skInit, skInitExport, skGetCampaigns } = require('./sk.service');

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
    const fetchStart = new Date();
    const pullDate = process.argv[2] ?? '';

    // const skResult = await skInit({ date: pullDate });
    const cxnResult = await cxnInit({ users: cxnUsers, date: pullDate });
    const fetchEnd = new Date();
    const timeToProcess = (fetchEnd - fetchStart) / 1000;

    const toWrite =
      JSON.stringify({
        // skResult,
        cxnResult,
        fetchStart,
        fetchEnd,
        timeToProcess,
        note: `only SK pull for ${pullDate}`,
      }) + '\n';

    await promiseWriteFile('logs.txt', toWrite, { flag: 'a' });

    console.log('--DONE--');
  } catch (error) {
    console.error('Error:', error.message);
  }
};

console.log('Scheduler started.');

fetchData();

// const test = async () => {
//   const camp = await skGetCampaigns();

//   console.log('camp.length :>> ', camp.length);
  // const pullDate = process.argv[2] ?? '';

  // await skInitExport({ date: pullDate });
// };

// test();

// cron.schedule('0 1 * * *', () => {
//   console.log('Running data pull...');
// });
