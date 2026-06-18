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

async function sendBatchMessages(items, defaultQueue) {
  const results = [];
  let sentCount = 0;
  let failedCount = 0;

  for (const [index, item] of items.entries()) {
    const targetQueue = item.queue || defaultQueue;
    const payload = { message: item.message, timestamp: new Date().toISOString() };

    const result = await sendMessageWithRetry(targetQueue, payload);

    if (result.success) {
      sentCount++;
      results.push({ index, success: true, queue: targetQueue, message: item.message });
    } else {
      failedCount++;
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

      results.push({
        index,
        success: false,
        queue: targetQueue,
        message: item.message,
        error: result.error,
        persisted: true,
      });
    }
  }

  return { total: items.length, sent: sentCount, failed: failedCount, results };
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

app.post("/send/batch", async (req, res) => {
  const { messages, queue } = req.body;

  if (!Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: "messages must be a non-empty array" });
  }

  const invalidItems = messages.filter((m, idx) => {
    if (!m || typeof m !== "object") return true;
    if (typeof m.message !== "string" || m.message.length === 0) return true;
    return false;
  });

  if (invalidItems.length > 0) {
    return res.status(400).json({
      error: "Each item in messages must be an object with a non-empty string 'message' field",
    });
  }

  const defaultQueue = queue || QUEUE_NAME;
  const batchResult = await sendBatchMessages(messages, defaultQueue);

  if (batchResult.failed === 0) {
    return res.json({ ...batchResult, allSucceeded: true });
  }

  res.status(207).json({ ...batchResult, allSucceeded: false });
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
