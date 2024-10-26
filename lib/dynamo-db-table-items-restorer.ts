
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as cr from "aws-cdk-lib/custom-resources";
import * as iam from "aws-cdk-lib/aws-iam";
import * as nodejsLambda from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";

export interface ResourceProperties {
  tableName: string;
}

export class DynamoDbTableItemsRestorer extends Construct {
  constructor(scope: Construct, id: string, props: {
    ddbTable: dynamodb.ITable,
  }) {
    super(scope, id);

    const lambdaFunction = new nodejsLambda.NodejsFunction(
      this,
      "ResourceHandler",
      {
        entry: "./resourceHandler.ts",
        handler: "handler",
        runtime: lambda.Runtime.NODEJS_20_X,
      }
    );
    lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        "dynamodb:Scan",
      ],
      // FIXME: リソースを指定したいが、旧テーブルはCDK上で参照できないため指定できない
      resources: ["*"],
    }))

    // データをコピーするので書き込みのみ
    props.ddbTable.grantWriteData(lambdaFunction);

    // カスタムリソースプロバイダーを作成
    const provider = new cr.Provider(this, "CustomResourceProvider", {
      onEventHandler: lambdaFunction,
    });
    
    new cdk.CustomResource(this, "CustomResource", {
      serviceToken: provider.serviceToken,
      properties: {
        // NOTE: テーブル名が変更した場合（=再作成される場合）にupdate処理が実行されるように
        tableName: props.ddbTable.tableName,
      } as ResourceProperties,
    });
  }
}