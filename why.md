# Why this project

rn the logs are json files, not real logs
- 
https://kafka.apache.org/quickstart/
## Kafka
Kafka is a distributed event streaming platform that lets you read, write, store, and process events (also called records or messages in the documentation) across many machines.

Example events are payment transactions, geolocation updates from mobile phones, shipping orders, sensor measurements from IoT devices or medical equipment, and much more. These events are organized and stored in topics. Very simplified, a topic is similar to a folder in a filesystem, and the events are the files in that folder.

### Kafka Use-cases
1) Messaging 
2. Website activity tracking
3. Metrics
4. log aggregation
5.Stream processing
6. event streaming
7. commit log



### Update april 8th 
So, have made basic consumer producer logs using kafka
and metrics observnig through redis

Step 1 
cd /home/yashasvi-sharma/Downloads/kafka_2.13-4.2.0

Step 2 - configure
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties

Step 3 
bin/kafka-server-start.sh config/server.properties

Step 4 
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs

#### Consumer logs

{"id":"ebc7586a-b8a7-4ebd-9cc1-405ae3e63c19","trace_id":"cac04aac-43b3-42b3-8145-c2b88730f3b7","timestamp":1775636304926,"service":"order","endpoint":"/status","method":"POST","status_code":200,"latency_ms":1157,"user_id":"user_2521","region":"ap-south-1","retry_count":0}
{"id":"8da60eec-363e-433d-9f5e-d818a2f34051","trace_id":"5bcce96e-e502-475a-812a-5b9f49a7985d","timestamp":1775636304926,"service":"payment","endpoint":"/pay","method":"POST


#### Producer logs
📤 Sent=1018 | base=1018 | factor=1.00
📤 Sent=1018 | base=1018 | factor=1.00
📤 Sent=1018 | base=1018 | factor=1.00
🚀 BURST x3
📤 Sent=3177 | base=1059 | factor=1.00
📤 Sent=1059 | base=1059 | factor=1.00
📤 Sent=1059 | base=1059 | factor=1.00