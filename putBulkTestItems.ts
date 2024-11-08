import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb"; // ES6 import
import { pipeline } from "stream/promises";
import { CdkCustomResourceHandler } from "aws-lambda";
import { Readable, Writable } from "node:stream";
import { Logger } from "@aws-lambda-powertools/logger";

const ddbClient = DynamoDBDocumentClient.from(new DynamoDB({}));

/** Lambdaのエントリーポイント */
export const main = async () => {
  // add 10000 items to the table
  for (let i = 0; i < 10000; i++) {
    const now = new Date();
    const putCommand = new PutCommand({
      TableName: "CdkTestStack-OriginalTable32190B7A-1Y23YOM6K651U",
      Item: {
        id: `dummy-${i.toString().padStart(5, "0")}`,
        createdAt: now.toISOString(),
        updatedAt: now.toISOString(),
      },
    });
    await ddbClient.send(putCommand);
    console.log(`Put item ${i}`);
  }
};

main();
