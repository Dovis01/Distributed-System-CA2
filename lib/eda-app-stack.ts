import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";

import {Construct} from "constructs";
import {Duration} from "aws-cdk-lib";

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Create the DynamoDB table
        const imagesTable = new dynamodb.Table(this, 'ImagesTable', {
            partitionKey: {name: 'FileName', type: dynamodb.AttributeType.STRING},
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            tableName: "Images",
        });

        const imagesBucket = new s3.Bucket(this, "images", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            publicReadAccess: false,
        });

        // Integration infrastructure
        const rejectMailerQueue = new sqs.Queue(this, "rejection-mailer-queue", {
            retentionPeriod: Duration.minutes(30),
        });

        const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
            receiveMessageWaitTime: cdk.Duration.seconds(10),
            deadLetterQueue: {
                queue: rejectMailerQueue,
                maxReceiveCount: 1,
            },
        });

        const newImageTopic = new sns.Topic(this, "NewImageTopic", {
            displayName: "New Image Topic",
        });

        const imagesTableTopic = new sns.Topic(this, "ImagesTableTopic", {
            displayName: "Images Table Topic",
        });

        // Lambda functions
        const processAddImageFn = new lambdanode.NodejsFunction(this, "ProcessAddImageFn", {
            runtime: lambda.Runtime.NODEJS_18_X,
            entry: `${__dirname}/../lambdas/processAddImage.ts`,
            timeout: cdk.Duration.seconds(15),
            memorySize: 128,
            environment: {
                TABLE_NAME: imagesTable.tableName,
                REGION: cdk.Aws.REGION,
            },
        });

        const processDeleteImageFn = new lambdanode.NodejsFunction(this, "ProcessDeleteImageFn", {
            runtime: lambda.Runtime.NODEJS_18_X,
            entry: `${__dirname}/../lambdas/processDeleteImage.ts`,
            timeout: cdk.Duration.seconds(15),
            memorySize: 128,
            environment: {
                TABLE_NAME: imagesTable.tableName,
                REGION: cdk.Aws.REGION,
            },
        });

        const processUpdateImageFn = new lambdanode.NodejsFunction(this, "ProcessUpdateImageFn", {
            runtime: lambda.Runtime.NODEJS_18_X,
            entry: `${__dirname}/../lambdas/processUpdateImage.ts`,
            timeout: cdk.Duration.seconds(15),
            memorySize: 128,
            environment: {
                TABLE_NAME: imagesTable.tableName,
                REGION: cdk.Aws.REGION,
            },
        });

        const mailerFn = new lambdanode.NodejsFunction(this, "mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/confirmMailer.ts`,
        });

        const rejectMailerFn = new lambdanode.NodejsFunction(this, "reject-mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/rejectMailer.ts`,
        });


        // S3 Bucket --> SNS Topic
        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.SnsDestination(newImageTopic)
        );

        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_REMOVED_DELETE,
            new s3n.SnsDestination(imagesTableTopic)
        );

        // Add SNS Topic subscriptions
        newImageTopic.addSubscription(new subs.SqsSubscription(imageProcessQueue));
        newImageTopic.addSubscription(new subs.LambdaSubscription(mailerFn));
        imagesTableTopic.addSubscription(new subs.LambdaSubscription(processDeleteImageFn, {
            filterPolicy: {
                comment_type: sns.SubscriptionFilter.stringFilter({
                    denylist: ['Caption']
                }),
            },
        }));
        imagesTableTopic.addSubscription(new subs.LambdaSubscription(processUpdateImageFn, {
            filterPolicy: {
                comment_type: sns.SubscriptionFilter.stringFilter({
                    allowlist: ['Caption']
                }),
            },
        }));

        // SQS --> Lambda
        const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });

        const rejectImageMailEventSource = new events.SqsEventSource(rejectMailerQueue, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });

        processAddImageFn.addEventSource(newImageEventSource);

        rejectMailerFn.addEventSource(rejectImageMailEventSource);

        mailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );

        rejectMailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );

        // Permissions
        imagesBucket.grantRead(processAddImageFn);
        imagesTable.grantReadWriteData(processAddImageFn);
        imagesTable.grantReadWriteData(processDeleteImageFn);
        imagesTable.grantReadWriteData(processUpdateImageFn);

        // Output
        new cdk.CfnOutput(this, "bucketName", {
            value: imagesBucket.bucketName,
        });
    }
}
