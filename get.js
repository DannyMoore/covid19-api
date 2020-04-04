import * as dynamoDbLib from "./libs/dynamodb-lib";
import { success, failure } from "./libs/response-lib";

export async function main(event, context) {

  const params = {
    TableName: "stats",
    KeyConditionExpression: "countryId = :countryId",
    ExpressionAttributeValues: {
        ":countryId": event.pathParameters.countryId
    }
  };

  try {
    console.log(params);

    const result = await dynamoDbLib.call("query",params);
    if (result) {
      // Return the retrieved item
      return success(result);
    } else {
      return failure({ status: false, error: "error." });
    }
  } catch (e) {
    console.log(e);
    return failure({ status: false });
  }
}