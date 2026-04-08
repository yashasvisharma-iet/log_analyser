const { Kafka } = require('kafkajs');
const pLimit = require('p-limit');

const { connectRedis, client } = require('../config/redisClient');
const { addToDLQ, isDuplicate } = require('../metrics/redisMetrics');

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-dlq' });
const concurrencyLimiter = pLimit(20);


async function startDLQConsumer() {
  logConsumerStartup();

  await initializeInfrastructure();
  await subscribeToDLQTopic();

  await beginMessageConsumption();

  attachConsumerLifecycleListeners();
}


async function initializeInfrastructure() {
  await connectRedis();
  await consumer.connect();
}

async function subscribeToDLQTopic() {
  await consumer.subscribe({ topic: 'logs-dlq', fromBeginning: false });
  console.log('📡 SUBSCRIBED TO logs-dlq');
}


async function beginMessageConsumption() {
  await consumer.run({
    autoCommit: false,
    eachMessage: routeMessageWithConcurrencyControl,
  });
}

async function routeMessageWithConcurrencyControl(kafkaContext) {
  await concurrencyLimiter(() => processDLQMessage(kafkaContext));
}

/**
 * CORE BUSINESS FLOW
 */
async function processDLQMessage({ topic, partition, message }) {
  const parsedPayload = safelyParseMessage(message);
  if (!parsedPayload) return;

  const { logEntry, failureReason } = extractDLQPayload(parsedPayload);
  if (!isProcessableLog(logEntry)) return;

  if (await isAlreadyProcessed(logEntry.id)) return;

  logDLQEvent(logEntry, failureReason);

  await persistDLQAndAcknowledgeOffset({
    topic,
    partition,
    message,
    logEntry,
    failureReason,
  });
}

/**
 * MESSAGE INTERPRETATION
 */
function safelyParseMessage(message) {
  try {
    return JSON.parse(message.value.toString());
  } catch {
    console.log('❌ DLQ PARSE ERROR — skipping');
    return null;
  }
}

function extractDLQPayload(payload) {
  return {
    logEntry: payload.log || payload,
    failureReason: payload.reason || 'UNKNOWN',
  };
}

function isProcessableLog(logEntry) {
  if (!logEntry?.id) {
    console.log('⚠️ INVALID DLQ MESSAGE — skipping');
    return false;
  }
  return true;
}

/**
 * IDEMPOTENCY CONTROL
 */
async function isAlreadyProcessed(logId) {
  const duplicate = await isDuplicate(`dlq:${logId}`);

  if (duplicate) {
    console.log(`⚠️ DUPLICATE DLQ ${logId} — skipping`);
  }

  return duplicate;
}

/**
 * LOGGING
 */
function logDLQEvent(logEntry, failureReason) {
  console.log(
    `💀 DLQ HIT: ${logEntry.id} | reason=${failureReason} | retries=${logEntry.retry_count || 0}`
  );
}

function logDLQSuccess(logEntry) {
  console.log(
    `✅ DLQ STORED: ${logEntry.id} (${Date.now() - logEntry.timestamp}ms total)`
  );
}

function logDLQFailure(error) {
  console.error('🔥 DLQ STORE FAILED:', error.message);
}

function logConsumerStartup() {
  console.log('🚀 DLQ CONSUMER STARTING...');
}

function logStartupFailure(error) {
  console.error('🔥 DLQ CONSUMER FAILED TO START:', error);
}

/**
 * PERSISTENCE + OFFSET MANAGEMENT
 */
async function persistDLQAndAcknowledgeOffset({
  topic,
  partition,
  message,
  logEntry,
  failureReason,
}) {
  try {
    await storeDLQRecord(logEntry, failureReason);
    await markLogAsProcessed(logEntry.id);

    logDLQSuccess(logEntry);

    await acknowledgeKafkaOffset(topic, partition, message.offset);
  } catch (error) {
    logDLQFailure(error);
  }
}

async function storeDLQRecord(logEntry, failureReason) {
  await addToDLQ(logEntry, failureReason);
}

async function markLogAsProcessed(logId) {
  await client.set(`dlq:processed:${logId}`, 1, { EX: 86400 });
}

async function acknowledgeKafkaOffset(topic, partition, offset) {
  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (Number(offset) + 1).toString(),
    },
  ]);
}

function attachConsumerLifecycleListeners() {
  consumer.on('consumer.crash', handleConsumerCrash);
  consumer.on('consumer.disconnect', handleConsumerDisconnect);
}

function handleConsumerCrash(event) {
  console.error('🔥 DLQ CONSUMER CRASHED:', event.payload);
}

function handleConsumerDisconnect() {
  console.warn('⚠️ DLQ CONSUMER DISCONNECTED');
}

startDLQConsumer().catch(logStartupFailure);