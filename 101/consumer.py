from confluent_kafka import Consumer, KafkaError

# Step 1: Configuration + Constructor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
c = Consumer(conf)

# Step 2: Topic subscription
c.subscribe(['mytopic'])

try:
    # Step 3: Endless Loop
    while True:
        # Step 4: poll message
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print(f"Received message: {msg.value()}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"End of partition reached {msg.topic()}/{msg.partition()}")
        else:
            print(f"Error occured: {msg.error().str()}")
except KeyboardInterrupt:
    pass
finally:
    # Step 5: Close consumer
    c.close()