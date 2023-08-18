const axios = require('axios');
const cron = require('node-cron');
const { format } = require('path');
const { MongoClient } = require('mongodb');
const path = require('path');
const { cxnInit } = require('./service');
const dotenv = require('dotenv');
const moment = require('moment');

dotenv.config();

console.log(`ENV = ${process.env.NODE_ENV}`);

const dbName = 'dailypull';
const caPath = '/home/ubuntu/global-bundle.pem';
let dev = true;
let uri = 'mongodb://localhost:27017';
let options = {};

if (dev) {
  options = {
    dbName,
    tls: true,
    tlsCAFile: path.resolve(caPath),
    replicaSet: 'rs0',
    readPreference: 'secondaryPreferred',
    retryWrites: 'false',
  };

  uri =
    'mongodb://adminuser:Admin123@daily-pull-vlm.cluster-ct3hcedret2a.eu-central-1.docdb.amazonaws.com:27017';
}

const client = new MongoClient(uri, {
  ...options,
});

const databaseName = 'dailypull'; // Replace with your database name
const skEmail = 'sean@aka-extensions.com';
const auth = 'Basic c2VhbkBha2EtZXh0ZW5zaW9ucy5jb206aHN1MjkzNGpkaXU=';
const apiUrlSourceKnowledge = 'https://api.sourceknowledge.com/affiliate/v2/campaigns';

const apiUrlConnexity = 'https://publisher-api.connexity.com/api/reporting/quality';
const currentDate = new Date();

const year = currentDate.getFullYear();
const month = String(currentDate.getMonth() + 1).padStart(2, '0');
const day = String(currentDate.getDate()).padStart(2, '0');

const formattedDate = `${year}-${month}-${day}`;

const users = [
  {
    username: 'rubi',
    publisherId: '726419',
    apiKey: '70c7f19d03b1421e18268119ed59805e',
  },
  {
    username: 'sean',
    publisherId: '726110',
    apiKey: '46d8b7532880e0856a913b04de86c2f2',
  },
];

const connectToMongoDB = async (dataSource, collectionName, user) => {
  try {
    await client.connect();
    console.log('Connected to MongoDB');

    // Get the database and collection references
    const database = client.db(databaseName);
    const collection = database.collection(collectionName);

    const yesterday = moment().subtract(1, 'day').format('YYYY-MM-DD');

    if (collectionName === 'stats_traffic_sources_new') {
      for (const data of dataSource) {
        let source = '';
        let winRatio = '';
        let clicks = '';
        let requests = '';
        let cpc = '';
        let cost = '';

        const response = await axios.get(
          `https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/${data.id}/by-publisher`,
          {
            headers: {
              Authorization: auth,
            },
            params: {
              from: yesterday,
              to: yesterday,
              page: '',
              channel: '',
              subid: '',
            },
          }
        );

        if (response.data.items.length > 0) {
          for (const info of response.data.items) {
            source = info.subId;
            winRatio = info.winRate;
            clicks = info.clicks;
            requests = info.clicks / info.winRate;
            cpc = info.ecpc;
            conversions = info.conversions;
            cost = info.spend;
            const res = {
              sourceName: 'Source Knowledge',
              sourceId: '',
              accountName: skEmail,
              accountId: '',
              createdAt: formattedDate,
              updatedAt: formattedDate,
              merchantName: data.advertiser.name,
              merchantId: data.advertiser.id,
              campaignId: data.id,
              campaignName: data.name,
              stats: {
                device: data.name,
                source,
                winRatio,
                clicks,
                requests,
                cpc,
                cost,
                conversions,
              },
            };
            const insertResult = await collection.insertOne(res);
            console.log('Inserted document:', insertResult.insertedId);
          }
        }
      }
    } else {
      let clicks = '';
      let epc = '';
      let revenue = '';
      let conversionRate = '';
      let costOfSale = '';
      let sales = '';
      let placementId = '';
      if (dataSource.length > 0) {
        for (const item of dataSource) {
          clicks = item.clicks;
          epc = item.cpc;
          revenue = item.clicks * item.cpc;
          conversionRate = item.conversionRate;
          costOfSale = item.costOfSale;
          sales = item.costOfSale ? revenue / costOfSale : 0;
          placementId = item.placementId.toString();
          const res = {
            sourceName: 'Connexity',
            sourceId: '',
            accountName: user,
            accountId: '',
            createdAt: formattedDate,
            updatedAt: formattedDate,
            merchantName: item.merchantName,
            merchantId: item.merchantId,
            stats: {
              clicks,
              epc,
              revenue,
              conversionRate,
              costOfSale,
              sales,
              placementId,
            },
          };
          const insertResult = await collection.insertOne(res);
          console.log('Inserted document:', insertResult.insertedId);
        }
      }
    }

    await client.close();
    console.log('Connection closed');
  } catch (error) {
    console.error('Error connecting to MongoDB:', error);
  }
};

const fetchData = async () => {
  try {
    const response = await axios.get(apiUrlSourceKnowledge, {
      headers: {
        Authorization: auth,
      },
      params: {
        page: null,
        advertiserId: null,
      },
    });

    // await connectToMongoDB(response.data.items, 'stats_traffic_sources_new');

    // await cxnInit();
  } catch (error) {
    console.error('Error:', error.message);
  }
};


console.log('Scheduler started.');

fetchData().finally(() => process.exit())
