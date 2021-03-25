#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

from dataclasses import dataclass
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


@dataclass
class User:
    """
    User model
    """
    name: str
    favorite_number: str
    favorite_color: str


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    :param:obj:dict: Object literal(dict)
    :param:ctx:SerializationContext: Metadata pertaining to the serialization operation.
    """
    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def main(args):
    topic = args.topic

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    schema_obj = schema_registry_client.get_latest_version(subject_name='example_serde_avro-value')

    avro_deserializer = AvroDeserializer(schema_obj.schema.schema_str,
                                         schema_registry_client,
                                         dict_to_user)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = msg.value()
            if user is not None:
                print(f"User record {msg.key()}: name: {user.name}\n"
                      f"\tfavorite_number: {user.favorite_color}\n"
                      f"\tfavorite_color: {user.favorite_number}\n")
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer Example client with "
                                                 "serialization capabilities")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")

    main(parser.parse_args())
