const kafka = require("./kafka");

const producer = kafka.producer();

async function startProducer() {
  try {
    await producer.connect();

    console.log("Kafka producer connected");

    const messages = Array.from({ length: 10 }, (_, index) => ({
      value: `Order ${index + 1}`,
    }));

    await producer.send({
      topic: "test-topic",
      messages,
    });

    console.log("Message sent successfully");
  } catch (error) {
    console.error("Failed to send message:", error);
    process.exitCode = 1;
  } finally {
    await producer.disconnect();
  }
}

startProducer();
