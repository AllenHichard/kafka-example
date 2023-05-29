from kafka import KafkaProducer
from enviroments import env


class Producer:
    def __init__(self):
        self.bootstrap_servers = env.bootstrap_servers

    def create_kafka_producer(self):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        return producer

    def send_message_to_topic(self, producer, topic, message):
        producer.send(topic, message.encode('utf-8'))

    def close_producer(self, producer):
        producer.close()


producer = Producer()
prod = producer.create_kafka_producer()
producer.send_message_to_topic(producer=prod, topic="chat", message="sync")
producer.close_producer(prod)