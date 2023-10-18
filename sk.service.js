const axios = require('axios');
const client = require('./db');
const moment = require('moment');

client.connect();

const SK = {
  URLS: {
    CAMPAIGN: 'https://api.sourceknowledge.com/affiliate/v2/campaigns',
  },
  AUTH: {
    BASIC: 'Basic c2VhbkBha2EtZXh0ZW5zaW9ucy5jb206aHN1MjkzNGpkaXU=',
  },
  CREDENTIALS: {
    EMAIL: 'sean@aka-extensions.com',
  },
};

exports.skGetCampaigns = async () => {
  const {
    data: { items },
  } = await axios.get(SK.URLS.CAMPAIGN, {
    headers: {
      Authorization: SK.AUTH.BASIC,
    },
    params: {
      page: null,
      advertiserId: null,
    },
  });

  return items;
};

exports.skGetStatsByDate = async ({ from, to, campaignId }) => {
  const {
    data: { items },
  } = await axios.get(
    `https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/${campaignId}/by-publisher`,
    {
      headers: {
        Authorization: SK.AUTH.BASIC,
      },
      params: {
        from,
        to,
        page: '',
        channel: '',
        subid: '',
      },
    }
  );

  return items;
};

exports.skCreateStat = ({
  skRawStat,
  accountEmail,
  createdDate,
  merchantName,
  merchantId,
  campaignId,
  campaignName,
}) => {
  const { subId, winRate, clicks, ecpc, conversions, spend } = skRawStat;

  const res = {
    sourceName: 'Source Knowledge',
    sourceId: '',
    accountName: accountEmail,
    accountId: '',
    createdAt: createdDate,
    updatedAt: createdDate,
    merchantName,
    merchantId,
    campaignId,
    campaignName,
    stats: {
      device: campaignName,
      source: subId,
      winRatio: winRate,
      clicks,
      requests: clicks / winRate,
      cpc: ecpc,
      cost: spend,
      conversions,
    },
  };

  return res;
};

exports.skSaveStat = async ({ stat }) => {
  const db = client.db('dailypull');
  const collection = db.collection('stats_traffic_sources_new');

  await collection.insertOne(stat);
};

exports._onInterval = async ({ interval, callback, callbackArgs }) => {
  const results = [];
  let count = 0;

  console.log(`Runs to finish: ${callbackArgs.length}`);

  for (const args of callbackArgs) {
    console.log(`Run ${count} start ----------------------`);
    const result = await callback(args);
    results.push(result);
    await new Promise((resolve, reject) => setTimeout(resolve, interval));
    count += 1;
    console.log(`---------------------- Run ${count} end`);
  }

  return results;
};

exports.skGetAndSaveStats = async ({ from, to, campaign, createdDate }) => {
  const {
    id: campaignId,
    name: campaignName,
    advertiser: { id: merchantId, name: merchantName },
  } = campaign;

  console.log(`Getting stat of ${campaignId}`);
  const skRawStats = await this.skGetStatsByDate({ from, to, campaignId });

  console.log('Raw Stats :>> ', skRawStats.length);

  for (const item of skRawStats) {
    const stat = this.skCreateStat({
      skRawStat: item,
      accountEmail: SK.CREDENTIALS.EMAIL,
      createdDate,
      merchantName,
      merchantId,
      campaignId,
      campaignName,
    });

    console.log(`Saving stat of ${campaignId}`);
    await this.skSaveStat({ stat });
  }

  return skRawStats.length;
};

exports.skInit = async () => {
  console.log('--skInit start--');
  console.time('skInit');

  const timeStart = new Date();
  const campaigns = await this.skGetCampaigns();

  const yesterday = moment(new Date('2023', '9', '11')).format('YYYY-MM-DD');
  const yesterdayDate = moment(new Date('2023', '9', '11'));

  const callbackArgs = campaigns.map(campaign => ({
    from: yesterday,
    to: yesterday,
    campaign,
    createdDate: new Date('2023', '9', '11'),
  }));

  const allRows = await this._onInterval({
    interval: 2000,
    callback: this.skGetAndSaveStats,
    callbackArgs,
  });

  const totalInsert = allRows.reduce((acc, item) => acc + item, 0);
  console.log('total inserted sk rows :>> ', totalInsert);

  const timeEnd = new Date();

  const timeToProcess = (timeEnd - timeStart) / 1000;
  console.timeEnd('skInit');

  return {
    rows: totalInsert,
    timeToProcess,
    args: {
      statsRequestedDate: {
        from: yesterday,
        to: yesterday,
        createdDate: yesterdayDate.toISOString(),
      },
    },
  };
};
