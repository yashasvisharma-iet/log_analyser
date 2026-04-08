const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

let baseRate = 1000;          // internal growth/shrink
let externalFactor = 1.0;     // from control loop

const MIN_RATE = 100;
const MAX_RATE = 5000;
const SERVICES = ['auth', 'payment', 'order'];
const ENDPOINTS = {
  auth: ['/login', '/signup'],
  payment: ['/pay', '/refund'],
  order: ['/create', '/status'],
};

const controlConsumer = kafka.consumer({ groupId: 'producer-control' });


const producer = kafka.producer();
//sends json msgs to kakfa topic

async function runProducer() {
  await initializeProducer();
  await startControlConsumer();
  await startProducingLogs();
}

async function initializeProducer() {
  await producer.connect();
  console.log("✅ Kafka Producer connected");

  process.on('SIGINT', handleShutdown);
  process.on('SIGTERM', handleShutdown);
}


async function handleShutdown() {
  console.log("🛑 Shutting down producer...");

  await producer.disconnect();
  console.log("❌ Kafka Producer disconnected");
  process.exit(0);
}

async function startProducingLogs() {
  while (true) {
    const rate = calculateFinalRate(baseRate, externalFactor);
    const messages = generateLogMessages(rate);

    //main thing to publish
    await publishLogs(messages);

    adjustBaseRateWithDrift();
    await waitBeforeNextBatch();
  }
}

//rate calculation 
function calculateFinalRate(baseRate, externalFactor) {
  const controlledRate = calculateControlledRate(baseRate, externalFactor);
  const burstAdjustedRate = applyTrafficBurst(controlledRate);

  return clampRate(burstAdjustedRate);
}

function calculateControlledRate(baseRate, externalFactor) {
  return baseRate * externalFactor;
}

function applyTrafficBurst(rate) {
  if (!isBurstTriggered()) return rate;

  const multiplier = random(2, 5);
  logBurst(multiplier);

  return rate * multiplier;
}

function isBurstTriggered() {
  return Math.random() < 0.1;
}

function clampRate(rate) {
  return Math.max(MIN_RATE, Math.min(MAX_RATE, Math.floor(rate)));
}

function logBurst(multiplier) {
  console.log(`🚀 BURST x${multiplier}`);
}

//message generation
function generateLogMessages(rate) {
  const messages = [];

  for (let i = 0; i < rate; i++) {
    messages.push(createKafkaMessageFromLog());
  }

  return messages;
}

function createKafkaMessageFromLog() {
  return {
    value: JSON.stringify(createLogPayload())
  };
}

function createLogPayload() {
  const service = pickService();
  return {
    id: uuidv4(),
    trace_id: uuidv4(),
    timestamp: Date.now(),
    service,
    endpoint: pickEndpoint(service),
    method: "POST",
    status_code: 200,
    latency_ms: realisticLatency(),
    user_id: resolveUserId(),
    region: "ap-south-1",
    retry_count: 0
  };
}

function resolveUserId() {
  return Math.random() < 0.02 ? null : "user_" + random(1, 5000);
}

//kafka
async function publishLogs(messages) {
  await producer.send({
    topic: 'logs',
    messages,
  });

  logBatchSent(messages.length);
}

function logBatchSent(count) {
  console.log(
    `📤 Sent=${count} | base=${Math.floor(baseRate)} | factor=${externalFactor.toFixed(2)}`
  );
}

async function startControlConsumer() {
  await controlConsumer.connect();
  await controlConsumer.subscribe({ topic: 'control-signals' });

  await controlConsumer.run({
    eachMessage: async ({ message }) => {
      const signal = JSON.parse(message.value.toString());

      if (signal.type === "RATE_ADJUST") {
        externalFactor = normalizeFactor(signal.factor);
        logControlUpdate(externalFactor);
      }
    }
  });
}

//miscellaneous
function adjustBaseRateWithDrift() {
  if (!shouldAdjustBaseRate()) return;

  baseRate *= random(95, 105) / 100;
  baseRate = limitRateWithinBounds(baseRate);
}

function shouldAdjustBaseRate() {
  return Math.random() < 0.3;
}

async function waitBeforeNextBatch() {
  await sleep(random(700, 1300));
}

function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pickService() {
  return SERVICES[random(0, SERVICES.length - 1)];
}

function pickEndpoint(service) {
  const list = ENDPOINTS[service] || ['/unknown'];
  return list[random(0, list.length - 1)];
}

function realisticLatency() {
  return random(10, 1200);
}

function normalizeFactor(factor) {
  const parsed = Number(factor);
  if (!Number.isFinite(parsed)) return 1.0;
  return Math.max(0.1, Math.min(3.0, parsed));
}

function logControlUpdate(factor) {
  console.log(`🎛️ Control update factor=${factor.toFixed(2)}`);
}

function limitRateWithinBounds(rate) {
  return Math.max(MIN_RATE, Math.min(MAX_RATE, rate));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

runProducer().catch((error) => {
  console.error('🔥 PRODUCER FAILED:', error);
  process.exit(1);
});
