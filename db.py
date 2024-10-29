import psycopg2
from config import DATABASE_URL

# Определение базы данных

def insertordertodb(order_event):
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(DATABASE_URL)
        cursor = connection.cursor()

        # Измененный доступ к данным
        order_id = order_event['order_id']
        customer_name = order_event['customer_name']
        customer_address = order_event['customer_address']
        amount = order_event['amount']
        status = order_event['status']

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO orders (order_id, customer_name, customer_address, amount, status)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (order_id, customer_name, customer_address, amount, status))
        connection.commit()
    except Exception as e:
        print(f"Ошибка при вставке заказа в БД: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()