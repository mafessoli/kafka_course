import PromptSync from 'prompt-sync';
import { Kafka, Partitioners } from 'kafkajs';

console.log("== Kafka Producer & Consumer ==");

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['127.0.0.1:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner});
const consumer = kafka.consumer({ groupId: 'test-group' });
const topicName = 'message-by-prompt';

const runProducer = async (message) => {
    await producer.connect();
    await producer.send({
        topic: topicName,
        messages: [{value: message }]
    });
    console.log("Message send successfully!");
    await producer.disconnect();
};

const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true});

    await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log("New Message Received");
        console.log({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString()
        });
    }});
};

await runConsumer();

const prompt = PromptSync({sigint: true});
const input = prompt('Write your message: ');

console.log("Sending message...");
await runProducer(input);

console.log("Producer finished. Keeping consumer alive...");