const moment = require('moment')


const from = moment(new Date('2023', '8', '17')).format('YYYY-MM-DD');
const yesterday = moment(new Date('2023', '8', '24')).format('YYYY-MM-DD');

console.log('from :>> ', from);
console.log('yesterday :>> ', yesterday);