import { SQSHandler } from "aws-lambda";
import {
    GetObjectCommand,
    GetObjectCommandInput,
    S3Client,
} from "@aws-sdk/client-s3";
import {DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";
import {DynamoDBClient, PutItemCommand} from "@aws-sdk/client-dynamodb";

const s3 = new S3Client();
const dynamoDbDocClient = createDynamoDbDocClient();

export const handler: SQSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const recordBody = JSON.parse(record.body);  // Parse SQS message
        const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message

        if (snsMessage.Records) {
            console.log("Record body ", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcBucket = s3e.bucket.name;
                // Object key may have spaces or unicode non-ASCII characters.
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));

                // Check if the image is a .jpeg or .png file
                if (!srcKey.endsWith('.jpeg') && !srcKey.endsWith('.png')) {
                    // Reject the image
                    throw new Error(`Unsupported file type: ${srcKey}`);
                }

                let originalImage = null;
                try {
                    // Download the image from the S3 source bucket.
                    const params: GetObjectCommandInput = {
                        Bucket: srcBucket,
                        Key: srcKey,
                    };
                    originalImage = await s3.send(new GetObjectCommand(params));
                    // Process the image ......
                    // Write the image file name to the DynamoDB table
                    const ddbParams = {
                        TableName: process.env.TABLE_NAME,
                        Item: {
                            FileName: { S: srcKey }
                        }
                    };
                    await dynamoDbDocClient.send(new PutItemCommand(ddbParams));
                } catch (error) {
                    console.log(error);
                }
            }
        }
    }
};

function createDynamoDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
