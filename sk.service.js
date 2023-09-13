const axios = require('axios');
const client = require('./db');
const moment = require('moment');

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

  return { campaignId, items };
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

exports.skBulkSaveStat = async ({ stats }) => {
  await client.connect();

  const db = client.db('dailypull');
  const collection = db.collection('stats_traffic_sources_new');

  await collection.insertMany(stats);
};

exports._onInterval = async ({ interval, callback, callbackArgs }) => {
  const results = [];
  let count = 0;

  console.log(`Runs to finish: ${callbackArgs.length}`);

  for (const args of callbackArgs) {
    console.log(`Run ${count} :>> ${JSON.stringify(args)}`);
    const result = await callback(args);
    results.push(result);
    await new Promise((resolve, reject) => setTimeout(resolve, interval));
    count += 1;
  }
  return results;
};

exports.skInit = async () => {
  console.log('--skInit start--');
  console.time('skInit');

  const campaigns = await this.skGetCampaigns();
  const campaignMap = campaigns.reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});

  const from = moment().subtract(2, 'weeks').format('YYYY-MM-DD');
  const yesterday = moment().subtract(1, 'day').format('YYYY-MM-DD');
  
  const yesterdayDate = moment().subtract(2, 'weeks').toDate();

  const callbackArgs = campaigns.map(item => {
    return {
      campaignId: item.id,
      from: from,
      to: yesterday,
    };
  });

  const campaignStats = await this._onInterval({
    interval: 3000,
    callback: this.skGetStatsByDate,
    callbackArgs: callbackArgs,
  });

  const campaignStatsRow = campaignStats.flatMap(campaign => {
    const { campaignId, items } = campaign;

    return items.map(item => {
      const matchCampaign = campaignMap[campaignId];

      return this.skCreateStat({
        skRawStat: item,
        accountEmail: SK.CREDENTIALS.EMAIL,
        createdDate: yesterdayDate,
        merchantId: matchCampaign.advertiser.id,
        merchantName: matchCampaign.advertiser.name,
        campaignId: matchCampaign.id,
        campaignName: matchCampaign.name,
      });
    });
  });

  await this.skBulkSaveStat({ stats: campaignStatsRow });

  console.timeEnd('skInit');
};
