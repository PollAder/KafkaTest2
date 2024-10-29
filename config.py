from confluent_kafka import Producer

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

DATABASE_URL = "postgresql://postgres:29122003mamaA!@127.0.0.1:5432/order_processing?client_encoding=UTF8"

# Конфигурация Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-processing-group',
    'auto.offset.reset': 'earliest'
}