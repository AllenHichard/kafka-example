from kafka import KafkaConsumer
from enviroments import env


class Consumer:

    def __init__(self):
        self.bootstrap_servers = env.bootstrap_servers

    def create_kafka_consumer(self, topic):
        consumer = KafkaConsumer(topic, bootstrap_servers=self.bootstrap_servers)
        return consumer

    def close_consumer(self, consumer):
        consumer.close()

    def list_messages_by_consumer(self, consumer):
        for messages in consumer:
            print(messages.value.decode('utf-8'))


consumer = Consumer()
client = consumer.create_kafka_consumer('chat')
consumer.list_messages_by_consumer(client)