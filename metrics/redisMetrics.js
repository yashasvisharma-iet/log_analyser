const { client } = require('../config/redisClient');

async function isDuplicate (id) {
  try {
    return await client.exists(`log:processed:${id}`);
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

async function markAsProcessed (id) {
  try {
    await client.set(`log:processed:${id}`, '1', { EX: 3600 });
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

async function updateMetrics ({
  service,
  window,
  isRetry,
  isFirstAttempt,
  retryCount,
  failed,
  latency,
  pipelineLatency,
  ingestionLatency
}) {
  const key = `metrics:${service}:${window}`;
  
  await client.sAdd(`metrics:windows:${service}`, window);
  await client.hIncrBy(key, 'total_attempts', 1);
  
  if (isFirstAttempt) {
    await client.hIncrBy(key, 'original_messages', 1);
  }
  
  if (isRetry) {
    await client.hIncrBy(key, 'retry_attempts', 1);
    await client.hIncrBy(key, 'total_retry_depth', retryCount);
  }
  
  if (failed) {
    await client.hIncrBy(key, 'total_failures', 1);
    
    if (isFirstAttempt) {
      await client.hIncrBy(key, 'first_attempt_failures', 1);
    } else {
      await client.hIncrBy(key, 'retry_failures', 1);
    }
  } else {
    if (isRetry) {
      await client.hIncrBy(key, 'retry_successes', 1);
    }
  }
  
  await client.hIncrBy(key, 'total_latency', latency || 0);
  await client.hIncrBy(key, 'total_pipeline_latency', pipelineLatency || 0);
  await client.hIncrBy(key, 'total_ingestion_latency', ingestionLatency || 0);
  
  await client.expire(key, 3600);
}


module.exports = {
  addToDLQ,
  isDuplicate,
  markAsProcessed,
  updateMetrics
};

async function addToDLQ(log, reason) {
  const key = `dlq:${log.id}`;
  await client.hSet(key, {
    payload: JSON.stringify(log),
    reason: reason || 'UNKNOWN',
    created_at: Date.now().toString(),
  });
  await client.expire(key, 86400);
}
