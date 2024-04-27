import { SQSHandler } from "aws-lambda";
import {DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";
import {DynamoDBClient, PutItemCommand} from "@aws-sdk/client-dynamodb";

const dynamoDbDocClient = createDynamoDbDocClient();

export const handler: SQSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const snsMessage = JSON.parse(record.body);  // Parse SNS message

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
                    throw new Error(`Unsupported file type: ${srcKey} in ${srcBucket}`);
                }

                try {
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
