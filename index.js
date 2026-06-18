const express = require("express");
const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = 3000;
const RABBITMQ_URL = "amqp://localhost";
const QUEUE_NAME = "test_queue";
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 1000;
const FAILED_LOG_PATH = path.join(__dirname, "failed_messages.json");

app.use(express.json());

let channel = null;
let connection = null;

function loadFailedLog() {
  try {
    if (fs.existsSync(FAILED_LOG_PATH)) {
      return JSON.parse(fs.readFileSync(FAILED_LOG_PATH, "utf-8"));
    }
  } catch (_) {}
  return [];
}

function persistFailedMessage(entry) {
  const log = loadFailedLog();
  log.push(entry);
  fs.writeFileSync(FAILED_LOG_PATH, JSON.stringify(log, null, 2), "utf-8");
  console.error(`Failed message persisted to ${FAILED_LOG_PATH}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectRabbitMQ() {
  connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  console.log(`Connected to RabbitMQ, queue "${QUEUE_NAME}" is ready`);

  connection.on("close", () => {
    console.error("RabbitMQ connection closed, attempting reconnect...");
    channel = null;
    connection = null;
    setTimeout(connectRabbitMQ, RETRY_BASE_DELAY_MS).catch(() => {});
  });

  connection.on("error", (err) => {
    console.error("RabbitMQ connection error:", err.message);
  });
}

async function sendMessageWithRetry(targetQueue, payload) {
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      if (!channel) {
        throw new Error("RabbitMQ channel not available");
      }

      await channel.assertQueue(targetQueue, { durable: true });

      const sent = channel.sendToQueue(
        targetQueue,
        Buffer.from(JSON.stringify(payload)),
        { persistent: true }
      );

      if (!sent) {
        throw new Error("Channel buffer full, write blocked");
      }

      return { success: true };
    } catch (err) {
      lastError = err;
      console.warn(
        `Send attempt ${attempt}/${MAX_RETRIES} failed for queue "${targetQueue}": ${err.message}`
      );

      if (attempt < MAX_RETRIES) {
        const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
        await sleep(delay);

        if (!channel) {
          try {
            await connectRabbitMQ();
          } catch (reconnErr) {
            console.warn(`Reconnect failed on attempt ${attempt}: ${reconnErr.message}`);
          }
        }
      }
    }
  }

  return { success: false, error: lastError.message };
}

app.post("/send", async (req, res) => {
  const { message, queue } = req.body;

  if (!message) {
    return res.status(400).json({ error: "message is required" });
  }

  const targetQueue = queue || QUEUE_NAME;
  const payload = { message, timestamp: new Date().toISOString() };

  const result = await sendMessageWithRetry(targetQueue, payload);

  if (result.success) {
    return res.json({ success: true, queue: targetQueue, message });
  }

  const failedEntry = {
    queue: targetQueue,
    payload,
    error: result.error,
    failedAt: new Date().toISOString(),
    retriesAttempted: MAX_RETRIES,
  };

  try {
    persistFailedMessage(failedEntry);
  } catch (logErr) {
    console.error("Failed to persist failed message log:", logErr.message);
  }

  res.status(503).json({
    error: "Message delivery failed after retries",
    detail: result.error,
    persisted: true,
  });
});

connectRabbitMQ()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Server running on http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Initial RabbitMQ connection failed:", err.message);
    process.exit(1);
  });
