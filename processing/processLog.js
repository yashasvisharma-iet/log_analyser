const { isDuplicate, markAsProcessed, updateMetrics } = require('../metrics/redisMetrics');
const { validate } = require('./validator');
const db = require('../db/fakeDb');


async function processLogEntry(log) {
  validate(log);
  log.retry_count = Number.isInteger(log.retry_count) ? log.retry_count : 0;

  if (await isDuplicate(log.id)) return;

  const context = buildExecutionContext(log);

  try {
    await executeLogWithTimeout(log);
    await handleSuccessfulProcessing(log, context);
  } catch (error) {
    await handleFailedProcessing(log, context, error);
  }
}

module.exports = { processLogEntry };

function buildExecutionContext(log) {
  return {
    service: log.service,
    window: calculateTimeWindow(log.timestamp),
    now: Date.now(),
    isRetry: log.retry_count > 0,
    isFirstAttempt: log.retry_count === 0
  };
}


async function executeLogWithTimeout(log) {
  await runWithTimeout(db.query(log), 1000);
}

async function handleSuccessfulProcessing(log, context) {
  await markAsProcessed(log.id);
  sendMetricsAsync(log, context, false);
  logSuccess(log);
}

async function handleFailedProcessing(log, context, error) {
  const errorType = classifyError(error);

  logFailure(log, errorType);
  sendMetricsAsync(log, context, true);

  throw createErrorResponse(error, errorType);
}


function calculateTimeWindow(timestamp) {
  return Math.floor(timestamp / 60000) * 60000;
}

async function runWithTimeout(promise, ms) {
  return Promise.race([
    promise,
    createTimeoutPromise(ms)
  ]);
}

function createTimeoutPromise(ms) {
  return new Promise((_, reject) =>
    setTimeout(() => reject(new Error("DB_TIMEOUT")), ms)
  );
}

function classifyError(error) {
  if (error.message === "DB_TIMEOUT") return "TEMPORARY";
  if (error.message.includes("connection")) return "TEMPORARY";
  return "PERMANENT";
}

function createErrorResponse(error, type) {
  return {
    type,
    message: error.message
  };
}

function sendMetricsAsync(log, context, failed) {
  updateMetrics({
    service: context.service,
    window: context.window,
    isRetry: context.isRetry,
    isFirstAttempt: context.isFirstAttempt,
    retryCount: log.retry_count,
    failed,
    latency: log.latency_ms,
    pipelineLatency: context.now - log.timestamp,
    ingestionLatency: log.ingestion_latency || 0
  }).catch(() => {});
}

function logSuccess(log) {
  console.log(`✅ SUCCESS ${log.id} retry=${log.retry_count}`);
}

function logFailure(log, errorType) {
  console.error(`🔥 FAILURE ${log.id} type=${errorType}`);
}
