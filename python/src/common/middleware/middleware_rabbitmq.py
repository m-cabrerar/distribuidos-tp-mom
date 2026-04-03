import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.channel = None

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        

    def send(self, message):
        try:    
            self.channel.basic_publish(exchange='',
                        routing_key=self.queue_name,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if self.channel:
                self.channel.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
