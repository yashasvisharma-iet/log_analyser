# Producer

## Setup 
Keep these two servers on terminal
bin/kafka-server-start.sh config/controller.properties
bin/kafka-server-start.sh config/broker.properties



## Haha
1. 🚀 Starts Producer + Control Loop
Connects Kafka producer
Starts a control consumer (important)
Begins infinite log generation loop
2. 📊 Generates Logs (Main Responsibility)

Inside startProducingLogs():

Calculates current rate
Generates that many log messages
Sends them to Kafka

👉 This simulates real production traffic

3. ⚙️ Dynamic Rate Control
Internal Control:
baseRate slowly changes (drift)
Random bursts (2x–5x spike)
Random timing between batches

👉 Simulates:

Traffic spikes
Natural fluctuations


## Main
await producer.send({
  topic: 'logs',
  messages,
});

👉 This is the actual producer responsibility:

Serialize logs
Push them into Kafka