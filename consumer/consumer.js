const { Kafka } = require('kafkajs');
const pLimit = require('p-limit');

const { connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');

const kafka = new Kafka({
  clientId: 'main-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-main' });
const producer = kafka.producer();

const limit = pLimit(50);

let paused = false;

async function runConsumer() {
  await initializeInfrastructure();
  await subscribeToTopic();
  await startConsuming();
}

async function initializeInfrastructure() {
  await connectRedis();
  await consumer.connect();
  await producer.connect();
}

async function subscribeToTopic() {
  await consumer.subscribe({ topic: 'logs', fromBeginning: false });
}


async function startConsuming() {
  await consumer.run({
    autoCommit: false,
    eachMessage: handleIncomingMessage,
  });
}

async function handleIncomingMessage(payload) {
  const { topic, partition, message } = payload;

  await limit(async () => {
    const log = parseMessage(message);
    await processMessage(log);
    await commitOffset(topic, partition, message.offset);
  });
}

// ================== PROCESSING ==================
function parseMessage(message) {
  const log = JSON.parse(message.value.toString());

  return enrichLog(log);
}

function enrichLog(log) {
  return {
    ...log,
    retry_count: log.retry_count || 0,
    ingestion_latency: Date.now() - log.timestamp,
  };
}

async function processMessage(log) {
  try {
    await processLog(log);
  } catch (error) {
      if (shouldRetry(log)) {
        await sendToRetryTopic(log);
      }
  }
}

function shouldRetry(log) {
  return log && log.retry_count === 0;
}

async function sendToRetryTopic(log) {
  await producer.send({
    topic: 'logs-retry',
    messages: [{
      value: JSON.stringify(createRetryPayload(log)),
    }],
  });
}

function createRetryPayload(log) {
  return {
    ...log,
    retry_count: 1,
    source: "RETRY",
  };
}

async function commitOffset(topic, partition, offset) {
  await consumer.commitOffsets([{
    topic,
    partition,
    offset: (Number(offset) + 1).toString(),
  }]);
}

global.pauseMainConsumer = async () => pauseConsumer();
global.resumeMainConsumer = async () => resumeConsumer();

function pauseConsumer() {
  if (!paused) {
    consumer.pause([{ topic: 'logs' }]);
    paused = true;
    console.log("⏸️ MAIN CONSUMER PAUSED");
  }
}

function resumeConsumer() {
  if (paused) {
    consumer.resume([{ topic: 'logs' }]);
    paused = false;
    console.log("▶️ MAIN CONSUMER RESUMED");
  }
}
runConsumer().catch(console.error);