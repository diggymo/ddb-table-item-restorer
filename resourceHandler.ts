import { DynamoDB } from "@aws-sdk/client-dynamodb"
import { BatchWriteCommand, DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb"; // ES6 import
import { pipeline } from 'stream/promises';
import { CdkCustomResourceHandler } from "aws-lambda";
import { Readable, Writable } from "node:stream";
import { Logger } from '@aws-lambda-powertools/logger';

const logger = new Logger();

const ddbClient = DynamoDBDocumentClient.from(new DynamoDB({}));

export interface ResourceProperties {
  tableName: string;
}


/** Lambdaのエントリーポイント */
export const handler: CdkCustomResourceHandler<ResourceProperties> = async (event) => {
  logger.info("リソースが変化しました", {event});
  switch (event.RequestType) {
    case "Create":
      return {};
    case "Update":
      await copyAllItems({
        fromTableName: event.OldResourceProperties.tableName,
        newTableName: event.ResourceProperties.tableName,
      });
      return {
        // DynamoDBが置換される場合、同様にこのリソースも置換される扱いにする
        // NOTE: 置換しないと、更新完了後に別リソースでエラー発生しRollbackが発生した場合、fromとtoが逆になりLambda関数が再実行されて無駄なデータコピーを引き起こしてしまうため
        PhysicalResourceId: event.ResourceProperties.tableName
      };
    case "Delete":
      return {};
  }
};


const copyAllItems = async (props: {
  fromTableName: string,
  newTableName: string,
}
) => {
  logger.info("データをコピーします", {fromTableName: props.fromTableName, newTableName: props.newTableName});
  const oldTableReadableStream = new DynamoDBReadableStream(ddbClient, props.fromTableName);
  const newTableWritableStream = new DynamoDBWritableStream(ddbClient, props.newTableName);
  await pipeline(oldTableReadableStream, newTableWritableStream)

  if (newTableWritableStream.errorChunks.length > 0) {
    logger.error("コピーに失敗したアイテムがあります", {errorChunks: newTableWritableStream.errorChunks})
    throw new Error(`${newTableWritableStream.errorChunks.length}件のデータをコピーできませんでした`)
  }
}


type StreamChunk = Record<string, unknown>[];

class DynamoDBWritableStream extends Writable {

  errorChunks: any[] = []

  constructor(
    private readonly ddbClient: DynamoDBDocumentClient,
    private readonly tableName: string,
  ) {
    super({objectMode: true});
  }

  override async _write(chunk: StreamChunk, encoding: BufferEncoding, callback: (error?: Error | null) => void): Promise<void> {
    try {
      const items = chunk.map(item => {
        return {
          PutRequest: {
            Item: item
          }
        }
      })
      const result = await this.ddbClient.send(
        new BatchWriteCommand({
          "RequestItems": {
            [this.tableName]: items
          }
        }),
      );

      if (result.UnprocessedItems !== undefined && result.UnprocessedItems[this.tableName] != null) {
        const unprocessedItems = result.UnprocessedItems[this.tableName].map(item => item.PutRequest?.Item)
        this.errorChunks.push(unprocessedItems)
      }
      callback()
    } catch(error) {
      callback(error as Error)
    }
  }
}


class DynamoDBReadableStream extends Readable {
  constructor(
    private readonly ddbClient: DynamoDBDocumentClient,
    private readonly tableName: string,
    private lastEvaluatedKey?: Record<string, unknown>
  ) {
    super({ objectMode: true });
  }

  override async _read() {
    const response = await this.ddbClient.send(
      new ScanCommand({
        TableName: this.tableName,
        ExclusiveStartKey: this.lastEvaluatedKey,
        // BatchWriteItems側が25件までしか書き込めないので、読み込みも25件ずつにする
        Limit: 25
      }),
    );
    if (response.Items != null && response.Items.length > 0) {
      this.push(response.Items);
    }

    this.lastEvaluatedKey = response.LastEvaluatedKey;

    if (this.lastEvaluatedKey == null) {
      logger.info("読み込みが完了しました")
      this.push(null);
    }
  }
}
