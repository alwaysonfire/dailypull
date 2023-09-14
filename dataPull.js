const cron = require('node-cron');

const { cxnInit } = require('./cxn.service');
const { skInit } = require('./sk.service');

const fetchData = async () => {
  try {
    // await skInit();
    await cxnInit();
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
