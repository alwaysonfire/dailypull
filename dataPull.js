const cron = require('node-cron');

const { cxnInit } = require('./cxn.service');
const { skInit } = require('./sk.service');

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
    await skInit();
    await cxnInit({ users: cxnUsers });
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
