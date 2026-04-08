# Kafka + Redis test checklist

## 1) Start dependencies

Open terminals and start:

1. Redis:
   - `redis-server`
2. Kafka (KRaft mode, based on existing notes):
   - `bin/kafka-server-start.sh config/controller.properties`
   - `bin/kafka-server-start.sh config/broker.properties`

## 2) Create required topics

```bash
bin/kafka-topics.sh --create --topic logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic logs-retry --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic logs-dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic control-signals --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 3) Start app processes

In separate terminals:

1. Main consumer: `node consumer/consumer.js`
2. DLQ consumer: `node consumer/dlqConsumer.js`
3. Producer: `node producer/producer.js`

## 4) Verify Kafka flow

Consume from main topic:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs --from-beginning
```

Consume from retry topic:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs-retry --from-beginning
```

Consume from DLQ topic:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs-dlq --from-beginning
```

## 5) Verify Redis state

```bash
redis-cli KEYS "log:processed:*"
redis-cli KEYS "metrics:*"
redis-cli KEYS "dlq:*"
```

Inspect a metric hash:

```bash
redis-cli HGETALL metrics:auth:<window_ms>
```

## 6) Control loop smoke test

Publish a control signal:

```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic control-signals
>{"type":"RATE_ADJUST","factor":0.5}
```

Producer logs should show changed `factor`.
