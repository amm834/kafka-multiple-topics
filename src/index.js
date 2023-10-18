import {Kafka, Partitioners} from 'kafkajs'

const kafka = new Kafka({
    clientId: 'test-app',
    brokers: ['localhost:9092']
})

const runProducer = async () => {
    const producer = kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner
    })
    await producer.connect()
    // await producer.send({
    //     topic: 'test-topic',
    //     messages: [
    //         {
    //             value: 'Hi, World!'
    //         },
    //     ]
    // })

    await producer.sendBatch({
        topicMessages: [
            {
                topic: 'topic-a',
                messages: [
                    {
                        value: 'Hello from TOPIC-A!'
                    },
                ]
            },
            {
                topic: 'test-b',
                messages: [
                    {
                        value: 'Hello from TOPIC-B!'
                    },
                ]
            }
        ]
    })
    await producer.disconnect()
}

const startConsumer = async () => {
    const consumer = kafka.consumer({groupId: 'group-id-1'})
    await consumer.connect()
    await consumer.subscribe({
        topics: ['topic-a', 'test-b'],
        fromBeginning: true,
    })

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            })
        }
    })
}

runProducer()
    .then(() => startConsumer())


