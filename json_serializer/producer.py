#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from uuid import uuid4
from dataclasses import dataclass
from six.moves import input

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


@dataclass
class User:
    """
    User record model
    """
    name: str
    favorite_number: int
    favorite_color: str
    address: str

def user_to_dict(user, ctx):
    """
    Converts object literal(dict) to a User instance.
    :param:obj:dict: Object literal(dict)
    :param:ctx:SerializationContext: Metadata pertaining to the serialization operation.
    """
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    :err:KafkaError: The error that occurred on None on success.
    :msg:Message: The message that was produced or failed.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def main(args):
    topic = args.topic

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "User",
      "description": "A Confluent Kafka Python User",
      "type": "object",
      "properties": {
        "name": {
          "description": "User's name",
          "type": "string"
        },
        "favorite_number": {
          "description": "User's favorite number",
          "type": "number",
          "exclusiveMinimum": 0
        },
        "favorite_color": {
          "description": "User's favorite color",
          "type": "string"
        }
      },
      "required": [ "name", "favorite_number", "favorite_color" ]
    }
    """
    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    json_serializer = JSONSerializer(schema_str, schema_registry_client, user_to_dict)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)

    print(f"Producing user records to topic {topic}. ^C to exit.")
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            # Get data
            user_name = input("Enter name: ")
            user_address = input("Enter address: ")
            user_favorite_number = int(input("Enter favorite number: "))
            user_favorite_color = input("Enter favorite color: ")
            # Build model
            user = User(name=user_name,
                        address=user_address,
                        favorite_color=user_favorite_color,
                        favorite_number=user_favorite_number)
            # Send data
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
    parser.add_argument('-t', dest="topic", default="example_serde_json",
                        help="Topic name")

    main(parser.parse_args())