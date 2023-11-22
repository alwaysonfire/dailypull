const winston = require('winston');
const WinstonCloudWatch = require('winston-cloudwatch');
const dotenv = require('dotenv');
dotenv.config();

const REGION = process.env.CLOUDWATCH_REGION;
const USER_ACCESS = process.env.CLOUDWATCH_USER_ACCESS;
const USER_SECRET = process.env.CLOUDWATCH_USER_SECRET;
const LOG_GROUP_NAME = process.env.CLOUDWATCH_LOG_GROUP_NAME;
const DAILYPULL_STREAM_NAME = 'dailypull';

const customLevel = {
  levels: {
    error: 0,
    info: 1,
    success: 2,
  },
};

const consoleOptions = {
  level: 'success',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss',
    }),
    winston.format.json()
  ),
};

const cloudWatchOptions = {
  logGroupName: LOG_GROUP_NAME,
  logStreamName: DAILYPULL_STREAM_NAME,
  awsOptions: {
    credentials: {
      accessKeyId: USER_ACCESS,
      secretAccessKey: USER_SECRET,
    },
    region: REGION,
  },
  messageFormatter: ({ level, message, metadata }) =>
    JSON.stringify({
      logLevel: level,
      message: message,
      parameters: metadata,
    }),
};

const logger = winston.createLogger({
  levels: customLevel.levels,
  transports: [
    new winston.transports.Console(consoleOptions),
    new WinstonCloudWatch(cloudWatchOptions),
  ],
  format: winston.format.metadata(info => {
    let message = `${JSON.stringify(info.obj)}`;
    return message;
  }),
});

module.exports = logger;
