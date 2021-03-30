import argparse
from uuid import uuid4

from six.moves import input

import user_pb2
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    :err:KafkaError: The error that occurred on None on success.
    :msg:Message: The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def main(args):
    topic = args.topic

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    protobuf_serializer = ProtobufSerializer(user_pb2.User,
                                             schema_registry_client)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': protobuf_serializer}

    producer = SerializingProducer(producer_conf)

    print(f"Producing user records to topic {topic}. ^C to exit.")
    while True:
        producer.poll(0.0)
        try:
            user_name = input("Enter name: ")
            user_favorite_number = int(input("Enter favorite number: "))
            user_favorite_color = input("Enter favorite color: ")
            user = user_pb2.User(name=user_name,
                                 favorite_color=user_favorite_color,
                                 favorite_number=user_favorite_number)
            producer.produce(topic=topic, key=str(uuid4()), value=user,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerializingProducer Example")

    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_protobuf",
                        help="Topic name")

    main(parser.parse_args())