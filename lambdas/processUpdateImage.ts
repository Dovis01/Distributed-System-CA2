import {SNSHandler} from "aws-lambda";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, GetCommand, UpdateCommand} from "@aws-sdk/lib-dynamodb";

const dynamoDbDocClient = createDynamoDbDocClient();

export const handler: SNSHandler = async (event: any) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const snsMessage = JSON.parse(record.Sns.Message);
        const attributes = record.Sns.MessageAttributes;
        console.log("SNS Message: ", snsMessage);

        if (attributes.comment_type && attributes.comment_type.Value === 'Caption') {
            const fileName = snsMessage.name;
            const description = snsMessage.description;

            const getItemParams = {
                TableName: process.env.TABLE_NAME,
                Key: {FileName: fileName},
            };

            try {
                const {Item} = await dynamoDbDocClient.send(new GetCommand(getItemParams));
                if (!Item) {
                    throw new Error(`Item with FileName "${fileName}" does not exist.`);
                }

                const updateItemParams = {
                    TableName: process.env.TABLE_NAME,
                    Key: {FileName: fileName},
                    UpdateExpression: 'set Description = :description',
                    ExpressionAttributeValues: {
                        ':description': description
                    }
                };

                await dynamoDbDocClient.send(new UpdateCommand(updateItemParams));
                console.log(`UpdateItem succeeded for ${fileName}`);
            } catch (error) {
                console.error(`Error updating item ${fileName}: `, error);
            }
        } else {
            console.log("No Caption attribute found in the message");
        }
    }
};

function createDynamoDbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}

