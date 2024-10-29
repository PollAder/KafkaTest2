from confluent_kafka import Producer, Consumer, KafkaError
import json
from db import insertordertodb  # Импортируем функцию вставки в БД
from config import producer_conf, consumer_conf

# Создание производителя и потребителя
producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)
consumer.subscribe(['orders'])

def process_order(order_data):
    # Разделяем строку на компоненты
    order_id, customer_name, customer_address, amount_str, status = order_data.split(', ')
    
    # Преобразуем amount в float
    amount = float(amount_str)

    return {
        "order_id": order_id,
        "customer_name": customer_name,
        "customer_address": customer_address,
        "amount": amount,
        "status": status
    }

def process_order(order_data):
    # Разделяем строку на компоненты
    components = order_data.split(', ')
    
    # Проверяем количество компонентов
    if len(components) != 5:
        print(f"Ошибка: ожидается 5 компонентов, но получено {len(components)}. Данные: {order_data}")
        return None  # Возвращаем None или обрабатываем ошибку по-другому

    order_id, customer_name, customer_address, amount_str, status = components
    
    try:
        # Преобразуем amount в float
        amount = float(amount_str)
    except ValueError:
        print(f"Ошибка преобразования amount: {amount_str}")
        return None  # Возвращаем None или обрабатываем ошибку по-другому

    return {
        "order_id": order_id,
        "customer_name": customer_name,
        "customer_address": customer_address,
        "amount": amount,
        "status": status
    }


def main():
    try:
        while True:
            msg = consumer.poll(1.0)  # Поллинг сообщений
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Ошибка: {msg.error()}")
                    break

            # Декодирование сообщения из строки с правильной кодировкой (UTF-8)
            raw_data = msg.value()
            print(f"Получено сообщение (в байтах): {raw_data}")  # Выводим полученное сообщение в байтах
            
            try:
                order_data = raw_data.decode('utf-8')  # Декодируем из UTF-8
                print(f"Получена строка заказа: {order_data}")  # Логируем полученную строку
                
                processed_order_event = process_order(order_data)  # Обрабатываем строку заказа
                
                print(f"Обработанный заказ: {processed_order_event}")  # Выводим обработанный заказ
                
                # Вставка обработанного заказа в базу данных
                insertordertodb(processed_order_event)

            except UnicodeDecodeError as e:
                print(f"Ошибка декодирования: {e}")
                continue

    except KeyboardInterrupt:
        pass
    
    finally:
        consumer.close()

if __name__ == "__main__":
    main()