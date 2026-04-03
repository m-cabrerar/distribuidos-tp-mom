import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.channel = None

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        self.channel.basic_publish(exchange='',
                      routing_key=self.queue_name,
                      body=message)

    def close(self):
        if self.channel:
            self.channel.close()
    
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            
            on_message_callback(body, ack, nack)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
