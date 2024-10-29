from config import producer

def create_order_event(order_id, name, address, amount, status):
    return f"{order_id}, {name}, {address}, {amount}, {status}"  # Возвращаем строку

def main():
    orders = [
        create_order_event("12345", "Ivan Ivanov", "New York 38", 10000, "NEW"),
        create_order_event("12346", "Petro Petrov", "Chicago Heart Blossom Alley1242", 5000, "NEW")
    ]

    for order_string in orders:
        # Отправляем строку в Kafka
        producer.produce('orders', value=order_string.encode('utf-8'))  # Кодируем в UTF-8 и отправляем данные в Kafka

    producer.flush()
    print("Orders sent")

if __name__ == "__main__":
    main()