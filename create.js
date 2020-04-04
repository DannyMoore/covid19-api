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

export async function main(event, context, callback) {

  let data = await getDataFromApi();

  if (data == null || data.length <= 0) {
    console.log("Empty reply from API");
    return;
  }

  let dataForDB = [];
  console.log(data);
  data.forEach(element => {
    let item = { ...element, countryId: element.country, timestamps: element.time, id: uuid.v1() };
    console.log(item);
    dataForDB.push({
      PutRequest: {
        Item: item
      }
    });
  });

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