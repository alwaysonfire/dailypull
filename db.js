const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const env = process.env.NODE_ENV?.trim() ?? 'dev'
const dbUrl = process.env.DB_URL;
const dbName = process.env.DB_NAME;
const dbCaFile = process.env.DB_CA_FILE_PATH;

const dbUser = process.env.DB_USER;
const dbPass = process.env.DB_PASSWORD;

let options = {};
let connectionUrl = 'mongodb://localhost:27017/dailypull';


if (true) {
  options = {
    dbName,
    tls: true,
    tlsCAFile: path.resolve(dbCaFile),
    replicaSet: 'rs0',
    readPreference: 'secondaryPreferred',
    retryWrites: 'false',
  };

  connectionUrl = `mongodb://${dbUser}:${dbPass}@${dbUrl}`;
}

console.log('DB URL :>> ', connectionUrl);
console.log('DB OPTIONS :>> ', options);

const client = new MongoClient(connectionUrl, options);

client.addListener('connecting', () => console.log('connecting to mongodb for dailypull'));
client.addListener('connected', () => console.log('connected to mongodb for dailypull'));

module.exports = client;
