const vlmClient = require('../db/vlm.db');
const dailyClient = require('../db');

const VLM_DB_NAME = process.env.VLM_DB_NAME;
const VLM_REDIRECT_TABLE = 'redirecttags';

const DAILY_DB_NAME = 'dailypull';
const PLACEMENT_TABLE = 'placementMapping';

const SK_TRAFFIC_ID = '6460e265e52729ef29b112f6';
const PLACEMENT_TAG = 'afPlacementId';
const RID_TAG = 'afRid';

const init = async () => {
  await vlmClient.connect();
  await dailyClient.connect();

  const vlmDb = await vlmClient.db(VLM_DB_NAME);
  const collection = vlmDb.collection(VLM_REDIRECT_TABLE);

  const dailyDb = await dailyClient.db(DAILY_DB_NAME);
  const mappingCollection = dailyDb.collection(PLACEMENT_TABLE);

  const docs = await collection.find({ trafficId: SK_TRAFFIC_ID }).toArray();

  const placementDocs = docs.filter(item => item.tag === PLACEMENT_TAG);
  const ridDocs = docs.filter(item => item.tag === RID_TAG);

  console.log('placementDocs', placementDocs.length);

  const ridDetails = Object.fromEntries(ridDocs.map(item => [item.originalValue, item.generatedValue]));

  const rows = placementDocs.map(item => generateMappingDoc(item, ridDetails)).filter(Boolean);

  console.log('rows', rows.length);

  const res = await mappingCollection.insertMany(rows);

  console.log('res.insertedCount', res.insertedCount);

  process.exit();
};

const generateMappingDoc = (doc, ridMap) => {
  const { generatedValue, originalValue, visitedUrl } = doc;

  const url = new URL(visitedUrl);
  const matchRid = url.searchParams.get(RID_TAG);

  if (!matchRid) return null;

  const generatedRid = ridMap[matchRid] ?? '';

  return {
    trafficId: SK_TRAFFIC_ID,
    campaignId: matchRid,
    sourceId: originalValue,
    rid: generatedRid,
    placementId: generatedValue,
  };
};

init();
