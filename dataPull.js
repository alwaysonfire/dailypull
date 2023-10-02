const cron = require('node-cron');

const { cxnInit } = require('./cxn.service');
const { skInit } = require('./sk.service');

const cxnUsers = [
  {
    email: 'rubi@aka-extensions.com',
    password: 'sdfLKNl34$jhndJ',
  },
];
const fetchData = async () => {
  try {

    await skInit();
    // await cxnInit({ users: cxnUsers });
  } catch (error) {
    console.error('Error:', error.message);
  }
};

console.log('Scheduler started.');

console.log('Running data pull...');
fetchData();

// cron.schedule('0 0 * * *', () => {
//   console.log('Running data pull...');
//   fetchData();
// });
