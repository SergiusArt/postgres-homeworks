"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import pandas

file_reader = pandas.read_csv('north_data/employees_data.csv')

# Подключаемся к БД
with psycopg2.connect(host='localhost',
                      database='north',
                      user='postgres',
                      password='3394') as conn:

    # Активируем курсор на первой таблице, для добавления в неё данных
    with conn.cursor() as cur:
        # Открываем csv файл для чтения данных employees_data
        with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
            head = True
            for line in file:
                # Если это заголовки, то пропускаем
                if head:
                    head = False
                # Остальные данные записываем в переменные
                else:
                    list_employees = list(line.split(',"'))
                    employee_id = int(list_employees[0])
                    first_name = list_employees[1][:-1]
                    last_name = list_employees[2][:-1]
                    title = list_employees[3][:-1]
                    birth_date = list_employees[4][:-1]
                    notes = list_employees[5][:-3]
                    line_table = (employee_id, first_name, last_name, title, birth_date, notes)
                    # Добавляем строчку с данными в таблицу БД
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', line_table)

        # Открываем csv файл для чтения данных customers_data
        with open('north_data/customers_data.csv', 'r', encoding='utf-8') as file:
            head = True
            for line in file:
                # Если это заголовки, то пропускаем
                if head:
                    head = False
                # Остальные данные записываем в переменные
                else:
                    list_customers = list(line.split(',"'))
                    customer_id = list_customers[1][:-1]
                    company_name = list_customers[1][:-1]
                    contact_name = list_customers[2][:-3]
                    line_table = (customer_id, company_name, contact_name)
                    # Добавляем строчку с данными в таблицу БД
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', line_table)

        # Открываем csv файл для чтения данных orders_data
        with open('north_data/orders_data.csv', 'r', encoding='utf-8') as file:
            head = True
            for line in file:
                # Если это заголовки, то пропускаем
                if head:
                    head = False
                # Остальные данные записываем в переменные
                else:
                    list_orders = list(line.split(','))
                    order_id = int(list_orders[0])
                    customer_id = list_orders[1][1:-1]
                    employee_id = int(list_orders[2])
                    order_date = list_orders[3]
                    ship_city = list_orders[4][1:-2]
                    line_table = (order_id, customer_id, employee_id, order_date, ship_city)
                    print(line_table)
                    # Добавляем строчку с данными в таблицу БД
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', line_table)

conn.close()
