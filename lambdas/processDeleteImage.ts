import { SNSHandler } from "aws-lambda";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DeleteCommand, DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";

const dynamoDbDocClient = createDynamoDbDocClient();
export const handler: SNSHandler = async (event: any) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const snsMessage = JSON.parse(record.Sns.Message);

        if (snsMessage.Records) {
            console.log("SNS Message: ", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const eventName = messageRecord.eventName;

                if (eventName.includes('ObjectRemoved:Delete')) {
                    const s3Info = messageRecord.s3;
                    const srcBucket = s3Info.bucket.name;
                    const srcKey = decodeURIComponent(s3Info.object.key.replace(/\+/g, " "));

                    try {
                        const deleteParams = {
                            TableName: process.env.TABLE_NAME,
                            Key: {
                                'FileName': srcKey
                            }
                        };
                        await dynamoDbDocClient.send(new DeleteCommand(deleteParams));
                        console.log(`Successfully deleted ${srcKey} in ${srcBucket} from DynamoDB table ${process.env.TABLE_NAME}`);
                    } catch (error) {
                        console.error("Error deleting item from DynamoDB", error);
                    }
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

