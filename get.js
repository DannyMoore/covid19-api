import * as dynamoDbLib from "./libs/dynamodb-lib";
import { success, failure } from "./libs/response-lib";

export async function main(event, context) {
  console.log("event: " + event)
  const params = {
    TableName: "history",
    KeyConditionExpression: "country = :country",
    ExpressionAttributeValues: {
        ":country": event.pathParameters.countryId
    },
    ScanIndexForward: true
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