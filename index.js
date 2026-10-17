import PromptSync from 'prompt-sync';
import { Kafka } from 'kafkajs';

console.log("== Kafka Producer ==");

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });
const topicName = 'message-by-prompt';

const runProducer = async (message) => {
    await producer.connect();
    await producer.send({
        topic: topicName,
        messages: [{value: message }]
    });
    await producer.disconnect();
};

await consumer.connect();
await consumer.subscribe({topic: topicName, fromBeginning: true});

await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({
            topic,
            offset: message.offset,
            value: message.value.toString()
        })
    }
});

const prompt = PromptSync({ sigint: true });
const input = prompt('Write your Message: ');

console.log("Sending message")
await runProducer(input);

console.log("End producer...");