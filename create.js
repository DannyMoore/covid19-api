import uuid from "uuid";
import * as dynamoDbLib from "./libs/dynamodb-lib";
import { success, failure } from "./libs/response-lib";
import axios from 'axios';

async function getDataFromApi() {
  return axios.get(process.env.RAPID_API_URL + '/history?country=Israel', {
    "headers": {
      "x-rapidapi-host": process.env.RAPID_API_HOST,
      "x-rapidapi-key": process.env.RAPIDAPI_ACCESS_KEY
    }
  })
    .then(response => {
      console.log(JSON.stringify(response.data.response));
      return response.data.response;
    })
    .catch(error => {
      console.log(error);
      return null;
    });
}

async function writeChunk(data) {
  let params = {
    RequestItems: {
      "stats": data
    }
  };

  try {
    await dynamoDbLib.call("batchWrite", params);
    console.log("API CAll ok");
    return success(params.Item);
  } catch (e) {
    console.log(e);
    return failure({ status: false });
  }
}

function getDBElement( dayTimestamp, lastUpdateTimestamp, element ) {
  let ret = {
    dayTimestamp: dayTimestamp,
    lastUpdateTimestamp: lastUpdateTimestamp,
    day: element.day,
    time: element.time,
    country: element.country,
    active: !isNaN(Number(element.cases.active)) ? Number(element.cases.active) : 0,
    critical: !isNaN(Number(element.cases.critical)) ? Number(element.cases.critical) : 0,
    recovered: !isNaN(Number(element.cases.recovered)) ? Number(element.cases.recovered) : 0,
    total: !isNaN(Number(element.cases.total)) ? Number(element.cases.total) : 0,
    deaths: !isNaN(Number(element.deaths.total)) ? Number(element.deaths.total) : 0,
    tests: !isNaN(Number(element.tests.total)) ? Number(element.tests.total) : 0,
  }

  return ret;
}

async function cleanData( data ) {
  let dataForDB = [];

  let map = new Map();


  data.forEach(element => {
    //let item = { ...element, countryId: element.country, timestamps: element.time, id: uuid.v1() };
    let dayTimestamp = Date.parse(element.day);
    let lastUpdateTimestamp = Date.parse(element.time);
    let key = element.country + "_:_" + element.day;

    let current = map.get(key);
    let newone = getDBElement(dayTimestamp, lastUpdateTimestamp, element);
    if ( current == undefined || newone.time > current.time) {
      console.log( "setting item: " + key + " to " + newone.time);
      map.set( key, newone)
    } else {
      console.log( "skipping item: " + key + newone.time);
    }
  });

  map.forEach( item => {
    console.log(item);
    //console.log(new Date(item.dayTimestamp).toDateString());
    //console.log(new Date(item.lastUpdateTimestamp).toTimeString());
    dataForDB.push({
      PutRequest: {
        Item: item
      }
    });
  });

  return dataForDB;
}

export async function main(event, context, callback) {

  let data = await getDataFromApi();

  if (data == null || data.length <= 0) {
    console.log("Empty reply from API");
    return;
  }
  // let sample = [{
  //   "country": "Israel",
  //   "cases": {
  //       "new": "+474",
  //       "active": 8262,
  //       "critical": 140,
  //       "recovered": 585,
  //       "total": 8904
  //   },
  //   "deaths": {
  //       "new": "+8",
  //       "total": 57
  //   },
  //   "tests": {
  //       "total": 109724
  //   },
  //   "day": "2020-04-07",
  //   "time": "2020-04-07T06:45:04+00:00"
  // }];

  // let data = sample;
  let dataForDB = await cleanData( data );
  console.log( "Got " + dataForDB.length + " items to update DB");

  // DynamoDB only accepts 25 items at a time.
  for (let i = 0; i < dataForDB.length; i += 25) {
    const upperLimit = Math.min(i + 25, dataForDB.length);
    const newItems = dataForDB.slice(i, upperLimit);
    try {
      await writeChunk(newItems);

    } catch (e) {
      console.log("Total Batches: " + Math.ceil(dataForDB.length / 25));
      console.error("There was an error while processing the request");
      console.log(e.message);
      console.log("Total data to insert", dataForDB.length);
      console.log("New items is", newItems);
      console.log("index is ", i);
      console.log("top index is", upperLimit);
      break;
    }
  }
  console.log(
    "If no errors are shown, DynamoDB operation has been successful"
  );

}