const kafka = require("./kafka");

const consumer = kafka.consumer({
  groupId: "kafka-group",
});

async function startConsumer() {
  try {
    await consumer.connect();

    console.log("Kafka consumer connected");

    await consumer.subscribe({
      topic: "test-topic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
      },
    });
  } catch (error) {
    console.error("Consumer error:", error);
    process.exitCode = 1;
  }
}

startConsumer();