#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

import user_pb2
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer


def main(args):
    topic = args.topic

    protobuf_deserializer = ProtobufDeserializer(user_pb2.User)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': protobuf_deserializer,
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
    parser = argparse.ArgumentParser(description="DeserializingConsumer Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_protobuf",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_protobuf",
                        help="Consumer group")

    main(parser.parse_args())