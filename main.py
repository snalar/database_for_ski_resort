#pip install faker
#pip install pymysql
#region Config
host = "localhost"
user = "root"
password = "password"
db_name = "ski_resort_new_version"
#endregion
#region Создание базы данных и прилигающих таблиц
import pymysql

def create_db_and_tables() -> None:
    def create_db() -> None:
        '''
        Создание базы данных
        '''
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                password=password
            )

            try:
                with connection.cursor() as cursor:
                    create_database_query = "CREATE DATABASE ski_resort_new_version"
                    cursor.execute(create_database_query)
                    connection.commit()
                    create_db.config(text="Database is created!")
            finally:
                connection.close()

        except Exception as ex:
            print("Database has already been created!")

    def created_table_clients() -> None:
        '''
        Создание таблицы clients
        '''
        try:
            connection = pymysql.connect(
                        host=host,
                        port=3306,
                        user=user,
                        password=password,
                        database=db_name,
                        cursorclass=pymysql.cursors.DictCursor
            )
            try:

                with connection.cursor() as cursor:
                    create_table_queary = ("CREATE TABLE clients (id_client int AUTO_INCREMENT,"
                                        " FIO varchar(255) NOT NULL,"
                                        " Date_of_birth date NOT NULL,"
                                        " Age varchar(32),"
                                        " PRIMARY KEY (id_client))")
                    cursor.execute(create_table_queary)
                    connection.commit()
                    print("Table clients is created!")

            finally:
                connection.close()

        except Exception as ex:
            print("Table clients has already been created!")

    def created_table_clients_stay_time() -> None:
        '''
        Создание таблицы users
        '''
        try:
            connection = pymysql.connect(
                        host=host,
                        port=3306,
                        user=user,
                        password=password,
                        database=db_name,
                        cursorclass=pymysql.cursors.DictCursor
            )
            try:

                with connection.cursor() as cursor:
                    create_table_queary = ("CREATE TABLE clients_stay_time (id_client int NOT NULL,"
                                        " arrival_date date NOT NULL,"
                                        " departure_date date NOT NULL,"
                                        " KEY id_client (id_client),"
                                        " FOREIGN KEY (id_client) REFERENCES clients (id_client))")
                    cursor.execute(create_table_queary)
                    connection.commit()
                    print("Table clients_stay_time is created!")

            finally:
                connection.close()

        except Exception as ex:
            print("Table clients_stay_time has already been created!")

    def created_table_services() -> None:
        '''
        Создание таблицы services
        '''
        try:
            connection = pymysql.connect(
                        host=host,
                        port=3306,
                        user=user,
                        password=password,
                        database=db_name,
                        cursorclass=pymysql.cursors.DictCursor
            )
            try:

                with connection.cursor() as cursor:
                    create_table_queary = ("CREATE TABLE services (id_service int NOT NULL AUTO_INCREMENT,"
                                        " name_service varchar(255) NOT NULL,"
                                        " price_service int NOT NULL,"
                                        " PRIMARY KEY (id_service))")
                    cursor.execute(create_table_queary)
                    connection.commit()
                    insert_query = "INSERT INTO services (id_service, name_service, price_service) VALUES (%s, %s, %s)"
                    services = [
                        (1, 'Аренда лыж и палок', 1500),
                        (2, 'Аренда сноуборда', 3500),
                        (3, 'Лыжного костюма', 1000),
                        (4, 'Костюма для сноуборда', 1000),
                        (5, 'Проживание', 10000),
                        (6, 'Сейф для личных вещей', 500)
                    ]
                    cursor.executemany(insert_query, services)
                    connection.commit()
                    print("Table services is created!")

            finally:
                connection.close()

        except Exception as ex:
            print("Table services has already been created!")

    def created_table_services_rendered() -> None:
        '''
        Создание таблицы services rendered
        '''
        try:
            connection = pymysql.connect(
                        host=host,
                        port=3306,
                        user=user,
                        password=password,
                        database=db_name,
                        cursorclass=pymysql.cursors.DictCursor
            )
            try:

                with connection.cursor() as cursor:
                    create_table_queary = ("CREATE TABLE services_rendered (id_client int NOT NULL,"
                                           " id_service int NOT NULL,"
                                           " KEY id_client (`id_client`),"
                                           " KEY id_service (`id_service`),"
                                           " CONSTRAINT services_rendered_ibfk_1 FOREIGN KEY (id_client) REFERENCES clients (id_client),"
                                           " CONSTRAINT services_rendered_ibfk_2 FOREIGN KEY (id_service) REFERENCES services (id_service))")
                    cursor.execute(create_table_queary)
                    connection.commit()
                    print("Table services rendered is created!")

            finally:
                connection.close()

        except Exception as ex:
            print("Table services rendered has already been created!")

    create_db()
    created_table_clients()
    created_table_clients_stay_time()
    created_table_services()
    created_table_services_rendered()
#endregion
#region Добавление случайных данных в таблицы
import pymysql
import random
import datetime
from faker import Faker

fake = Faker('ru_RU')

def random_data() -> None:
    '''
    Добавление случайных данных в таблицу
    '''
    try:
        connection = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:

                cursor.execute("SELECT MAX(id_client) FROM clients")
                result = cursor.fetchone()

                if result['MAX(id_client)'] != None:
                    start = result['MAX(id_client)'] + 1
                    end = start + 10_001
                else:
                    start = 1
                    end = 10_001

                for i in range(start, end):
                    fn = fake.name()
                    fdb = fake.date_between(start_date="-40y", end_date="-20y")
                    fage = 2023 - int(fdb.year)
                    insert_query_clients = "INSERT INTO clients(id_client, FIO, date_of_birth, Age) VALUES (%s, %s, %s, %s)"
                    cursor.execute(insert_query_clients, (i, fn, fdb, fage))
                    connection.commit()
                for i in range(start, end):
                    fdb1 = fake.date_between(start_date="-1y", end_date="now")
                    fdb2 = datetime.date(fdb1.year, fdb1.month, fdb1.day - 7 if fdb1.day - 7 > 0 else fdb1.day + 7)
                    fdb = sorted([fdb1, fdb2])
                    insert_query_clients_stay_time = "INSERT INTO clients_stay_time(id_client, arrival_date, departure_date) VALUES (%s, %s, %s)"
                    cursor.execute(insert_query_clients_stay_time, (i, fdb[0], fdb[1]))
                    connection.commit()
                for i in range(start, end):
                    idsidc = []
                    for _ in range(random.randint(1, 6)):
                        idsidc += [(i, random.randint(1, 6))]
                    idsidc = sorted(set(idsidc))
                    for j in range(len(idsidc)):
                        insert_query_services_rendered = "INSERT INTO services_rendered(id_client, id_service) VALUES (%s, %s)"
                        cursor.execute(insert_query_services_rendered, (idsidc[j][0], idsidc[j][1]))
                        connection.commit()
                print("Random data has been added to the tables")

        finally:
            connection.close()


    except Exception as ex:
        print("Error404")
        print(ex)
#endregion
#region SQL-запрос
import pymysql
def sql_request():
    def execute_query(host, user, password, db_name):
        try:
            connection = pymysql.connect(
                host=host,
                port=3306,
                user=user,
                password=password,
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor
            )

            with connection.cursor() as cursor:
                query = """
                SELECT clients.FIO AS 'ФИО',
                GROUP_CONCAT(services.name_service SEPARATOR ', ') AS 'Оказанные услуги',
                SUM(services.price_service) AS 'Стоимость услуг',
                clients_stay_time.arrival_date AS 'Дата приезда',
                clients_stay_time.departure_date AS 'Дата отъезда'
                FROM clients
                JOIN clients_stay_time ON clients.id_client = clients_stay_time.id_client
                JOIN services_rendered ON clients.id_client = services_rendered.id_client
                JOIN services ON services_rendered.id_service = services.id_service
                GROUP BY clients.FIO, clients_stay_time.arrival_date, clients_stay_time.departure_date
                ORDER BY clients.FIO;
                """
                cursor.execute(query)
                result = cursor.fetchall()

                return result

        except Exception as ex:
            print(f"Error: {ex}")
            return None

        finally:
            connection.close()


    result = execute_query(host, user, password, db_name)

    if result:
        for row in result:
            output_str = (
                f"ФИО: {row['ФИО']}, "
                f"Оказанные услуги: {row['Оказанные услуги']}, "
                f"Стоимость услуг: {row['Стоимость услуг']}, "
                f"Дата приезда: {row['Дата приезда']}, "
                f"Дата отъезда: {row['Дата отъезда']}"
            )
            print(output_str)
    else:
        print("Query execution failed.")
#endregion
#region Удаление БД
def drop_db() -> None:
    def drop_db_query() -> None:
        '''
        Удаление базы данных
        '''
        try:
            connection = pymysql.connect(
                host=host,
                user=user,
                password=password
            )

            try:
                with connection.cursor() as cursor:
                    create_database_query = "DROP DATABASE ski_resort_new_version"
                    cursor.execute(create_database_query)
                    connection.commit()
                    print("Database is deleted!")
            finally:
                connection.close()

        except Exception as ex:
            print("Database has already been deleted!")

    drop_db_query()
#endregion
#region Приложение
import pymysql
import tkinter as tk

def application() -> None:

    def submit_form() -> None:

        try:
            connection = pymysql.connect(
                host=host,
                port=3306,
                user=user,
                password=password,
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor
            )

            with connection.cursor() as cursor:
                cursor.execute("SELECT MAX(id_client) FROM clients")
                result = cursor.fetchone()
                max_id_client = result['MAX(id_client)']
                id_client = max_id_client + 1 if max_id_client is not None else 1

                insert_query_clients = "INSERT INTO clients(id_client, FIO, date_of_birth, Age) VALUES (%s, %s, %s, %s)"
                cursor.execute(insert_query_clients, (id_client,
                                                      fio_entry.get(),
                                                      dob_entry.get(),
                                                      age_entry.get()))
                connection.commit()

                insert_query_clients_stay_time = "INSERT INTO clients_stay_time(id_client, arrival_date, departure_date) VALUES (%s, %s, %s)"
                cursor.execute(insert_query_clients_stay_time, (id_client,
                                                                arrival_entry.get(),
                                                                departure_entry.get()))
                connection.commit()

                selected_services = []
                if service1_var.get() == 1:
                    selected_services.append(1)
                if service2_var.get() == 1:
                    selected_services.append(2)
                if service3_var.get() == 1:
                    selected_services.append(3)
                if service4_var.get() == 1:
                    selected_services.append(4)
                if service5_var.get() == 1:
                    selected_services.append(5)
                if service6_var.get() == 1:
                    selected_services.append(6)

                for service in selected_services:
                    insert_query_services_rendered = "INSERT INTO services_rendered(id_client, id_service) VALUES (%s, %s)"
                    cursor.execute(insert_query_services_rendered, (id_client, service))
                    connection.commit()

        except Exception as ex:
            print(ex)
            print("Connection refused...")

        else:
            print("Запись добавлена!")

    window = tk.Tk()
    window.title("Ski Resort Form")

    create_db_button = tk.Button(window, text="Create database", command=create_db_and_tables)
    create_db_button.pack()

    add_random_data_button = tk.Button(window, text="Add random data in database", command=random_data)
    add_random_data_button.pack()

    fio_label = tk.Label(window, text="ФИО клиента:")
    fio_label.pack()

    fio_entry = tk.Entry(window, width=50)
    fio_entry.pack()

    dob_label = tk.Label(window, text="Дата рождения:")
    dob_label.pack()
    dob_entry = tk.Entry(window, width=50)
    dob_entry.pack()

    age_label = tk.Label(window, text="Возраст клиента:")
    age_label.pack()
    age_entry = tk.Entry(window, width=50)
    age_entry.pack()

    arrival_label = tk.Label(window, text="Дата приезда:")
    arrival_label.pack()
    arrival_entry = tk.Entry(window, width=50)
    arrival_entry.pack()

    departure_label = tk.Label(window, text="Дата отъезда:")
    departure_label.pack()
    departure_entry = tk.Entry(window, width=50)
    departure_entry.pack()

    services_label = tk.Label(window, text="Выберите услуги:")
    services_label.pack()
    service1_var = tk.IntVar()
    service1 = tk.Checkbutton(window, text="Аренда лыж и палок", variable=service1_var)
    service1.pack()
    service2_var = tk.IntVar()
    service2 = tk.Checkbutton(window, text="Аренда сноуборда", variable=service2_var)
    service2.pack()
    service3_var = tk.IntVar()
    service3 = tk.Checkbutton(window, text="Аренда лыжного костюма", variable=service3_var)
    service3.pack()
    service4_var = tk.IntVar()
    service4 = tk.Checkbutton(window, text="Аренда костюма для сноуборда", variable=service4_var)
    service4.pack()
    service5_var = tk.IntVar()
    service5 = tk.Checkbutton(window, text="Проживание", variable=service5_var)
    service5.pack()
    service6_var = tk.IntVar()
    service6 = tk.Checkbutton(window, text="Сейф для личных вещей", variable=service6_var)
    service6.pack()

    submit_button = tk.Button(window, text="Submit", command=submit_form)
    submit_button.pack()

    sql_zapros = tk.Button(window, text="SQL-request", command=sql_request)
    sql_zapros.pack()

    delete_db = tk.Button(window, text="Delete database", command=drop_db)
    delete_db.pack()

    window.mainloop()
#endregion

if __name__ == '__main__':
    application()