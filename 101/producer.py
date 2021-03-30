from confluent_kafka import Producer

# Step 1: Configuration + Producer instance
conf = {'bootstrap.servers': 'localhost:9092'}
p = Producer(conf)

# Sending message with callback
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    :param:err:KafkaError: The error that occurred on None on success.
    :param:msg:Message: The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Step 2: Produce message
p.produce('mytopic', key='hello', value='world', on_delivery=delivery_report)

# Step 3: send message
p.flush(30)
