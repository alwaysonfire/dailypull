const axios = require('axios');
const dotenv = require('dotenv');
const Papa = require('papaparse');
const client = require('./db');
const moment = require('moment');

dotenv.config();

const CXN_BASE_URL = 'https://publisher-api.connexity.com';

const CXN = {
  URLS: {
    BASE: CXN_BASE_URL,
    AUTHENTICATE: `${CXN_BASE_URL}/authenticate`,
    GENERATE_REPORT: `${CXN_BASE_URL}/reporting/generateReports`,
  },
  REPORT: {
    COLUMNS: {
      FROM_DATE: 'createdAt',
      TO_DATE: 'updatedAt',
      MERCHANT_ID: 'merchantId',
      MERCHANT_NAME: 'merchantName',
      PLACEMENT_ID: 'stats.placementId',
      RID: 'stats.rid',
      CLICKS: 'stats.clicks',
      EARNINGS: 'stats.earnings',
      CONVERSION_RATE: 'stats.conversionRate',
      CPC: 'stats.epc',
      SALES_COUNT: 'stats.sales',
    },
  },
};

exports._cxnCleanJson = ({ data, email, accountId = '' }) => {
  return data.map(item => {
    const {
      FROM_DATE,
      MERCHANT_ID,
      MERCHANT_NAME,
      PLACEMENT_ID,
      RID,
      CLICKS,
      EARNINGS,
      CONVERSION_RATE,
      CPC: EPC,
      SALES_COUNT,
    } = item;

    const newData = {
      sourceName: 'connexity',
      sourceId: '',
      accountName: email,
      accountId,
      createdAt: moment(FROM_DATE).toDate(),
      updatedAt: moment(FROM_DATE).toDate(),
      merchantName: MERCHANT_NAME,
      merchantId: MERCHANT_ID,
      campaignId: '',
      campaignName: '',
      stats: {
        clicks: +CLICKS,
        epc: +EPC,
        revenue: +EARNINGS,
        conversionRate: +CONVERSION_RATE,
        conversions: Math.round(CLICKS * CONVERSION_RATE),
        costOfSale: '',
        sales: +SALES_COUNT,
        placementId: PLACEMENT_ID,
        rid: RID,
      },
    };

    return newData;
  });
};

exports.cxnAuthenticate = async ({ email, password }) => {
  const { data } = await axios.post(CXN.URLS.AUTHENTICATE, { email, password, publisherId: 0 });
  return data;
};

// to pull 10-09

exports.cxnGetReportDownloadUrl = ({ token, publisherId, retry = 5 }, done) => {
  console.log('downloading connexity report');

  return new Promise(async (resolve, reject) => {
    const requestData = {
      publisherId: publisherId.toString(),
      reportType: 'CUSTOM_REPORT',
      timeRangeType: null,
      startDate: '2023-10-09',
      endDate: '2023-10-09',
      aggregationType: 'DAY',
      pageNumber: 1,
      preview: false,
      fieldList: 'PLACEMENT_ID,RID,MERCHANT_ID,MERCHANT_NAME',
    };

    const { data } = await axios.post(CXN.URLS.GENERATE_REPORT, requestData, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const {
      reportResponse: { reportUrls },
    } = data;

    const [downloadUrl] = reportUrls;

    const resolveFn = done ?? resolve;

    if (downloadUrl) {
      return resolveFn(downloadUrl);
    }

    if (retry > 0) {
      console.log(`retrying to download connexity report. retries left: ${retry - 1}`);

      setTimeout(
        () => this.cxnGetReportDownloadUrl({ token, publisherId, retry: retry - 1 }, resolveFn),
        10000
      );
    } else {
      reject('no data returned by connexity');
    }
  });
};

exports.cxnGetReportJsonData = async ({ email, password, accountId }) => {
  const { token, publisherId } = await this.cxnAuthenticate({ email, password });

  const data = await this.cxnGetReportDownloadUrl({ token, publisherId });

  if (!data) throw new Error('no report generated from connexity');

  const { url: downloadUrl } = data;

  const json = await this.downloadCsvAsJson({ downloadUrl, email, accountId });

  return json;
};

exports.downloadCsvAsJson = async ({ downloadUrl, config = {}, email, accountId }) => {
  const { data } = await axios.get(downloadUrl, config);

  const json = Papa.parse(data, { header: true });

  json.data = json.data.filter(item => item.MERCHANT_NAME);

  const cleanedJson = this._cxnCleanJson({ data: json.data, email, accountId });

  return cleanedJson;
};

exports.cxnInit = async ({ users }) => {
  console.log('Pulling from connexity');
  console.time('connexity_time');
  const timeStart = new Date();

  await client.connect();

  let totalCount = 0;

  for (const user of users) {
    console.log(`getting data from ${user.email}`);

    const cxnData = await this.cxnGetReportJsonData(user);

    console.log(`data from ${user.email} :>> `, cxnData.length);

    const db = client.db('dailypull');
    const collection = db.collection('stats_media_platforms_new');

    console.log(`saving ${cxnData.length} cxn data`);
    if (cxnData.length > 0) {
      await collection.insertMany(cxnData);
    }

    totalCount += cxnData.length;
  }

  console.log('total inserted cxn rows :>> ', totalCount);

  console.timeEnd('connexity_time');
  const timeEnd = new Date();

  const timeToProcess = (timeEnd - timeStart) / 1000;
  console.timeEnd('skInit');

  return {
    rows: totalCount,
    timeToProcess,
  };
};
