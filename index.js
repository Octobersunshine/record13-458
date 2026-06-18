const express = require("express");
const amqp = require("amqplib");

const app = express();
const PORT = 3000;
const RABBITMQ_URL = "amqp://localhost";
const QUEUE_NAME = "test_queue";

app.use(express.json());

let channel = null;

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    console.log(`Connected to RabbitMQ, queue "${QUEUE_NAME}" is ready`);
  } catch (err) {
    console.error("Failed to connect to RabbitMQ:", err.message);
    process.exit(1);
  }
}

app.post("/send", async (req, res) => {
  const { message, queue } = req.body;

  if (!message) {
    return res.status(400).json({ error: "message is required" });
  }

  if (!channel) {
    return res.status(503).json({ error: "RabbitMQ channel not available" });
  }

  const targetQueue = queue || QUEUE_NAME;

  try {
    await channel.assertQueue(targetQueue, { durable: true });
    const sent = channel.sendToQueue(
      targetQueue,
      Buffer.from(JSON.stringify({ message, timestamp: new Date().toISOString() })),
      { persistent: true }
    );

    if (!sent) {
      return res.status(503).json({ error: "Message could not be sent, channel buffer full" });
    }

    res.json({ success: true, queue: targetQueue, message });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

connectRabbitMQ().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
});
