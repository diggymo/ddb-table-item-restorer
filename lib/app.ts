import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { DynamoDbTableItemsRestorer } from './dynamo-db-table-items-restorer';

export class AppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const originalTable = new dynamodb.Table(this, 'OriginalTable', {
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })

    new DynamoDbTableItemsRestorer(this, 'OriginalTableItemsRestorer', {
      ddbTable: originalTable
    })
  }
}
