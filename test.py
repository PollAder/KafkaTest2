import json
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
from psycopg2 import sql

# Настройки для подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Укажите адрес вашего Kafka
KAFKA_TOPIC = 'order-processing-group'        # Укажите название топика

# Настройки для подключения к PostgreSQL
DB_HOST = '127.0.0.1'
DB_NAME = 'order_processing'
DB_USER = 'postgres'
DB_PASSWORD = '"29122003mamaA!"'

# Функция для отправки данных в Kafka
def send_data_to_kafka(producer):
    while True:
        data = {
            "city": "SPb",
            "temperature": 42,
            "condition": "Sunny"
        }
        try:
            producer.send(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
            print(f"Sent data: {data}")
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")
        time.sleep(10)  # Отправка данных каждые 10 секунд

# Функция для получения данных из Kafka и записи в PostgreSQL
def consume_and_store_data(consumer, db_connection):
    cursor = db_connection.cursor()
    try:
        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                print(f"Received data: {data}")

                # Запись в базу данных
                cursor.execute(
                    sql.SQL("INSERT INTO weather_data (city, temperature, condition) VALUES (%s, %s, %s)"),
                    (data['city'], data['temperature'], data['condition'])
                )
                db_connection.commit()
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        cursor.close()

# Основная функция
def main():
    # Создание продюсера Kafka
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # Инициализация переменной db_connection
    db_connection = None

    try:
        # Создание соединения с PostgreSQL
        db_connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        # Запуск отправки данных в отдельном потоке
        threading.Thread(target=send_data_to_kafka, args=(producer,), daemon=True).start()

        # Создание потребителя Kafka
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id='weather_group'
        )

        # Получение и запись данных из Kafka в PostgreSQL
        consume_and_store_data(consumer, db_connection)

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
    finally:
        if db_connection is not None:
            db_connection.close()
        producer.close()

if __name__ == "__main__":
    main()