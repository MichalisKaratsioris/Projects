import pyodbc

# for driver in pyodbc.drivers():
#     print(driver)

try:
    connection = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=cars;"
        "Trusted_Connection=yes;"
        # "UID=username;"
        # "PWD=password;"
        "", timeout=1
    )

    cursor = connection.cursor()

    import json
    import os

    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, 'scores_sql.json')
    with open(configuration_file_path, 'r') as f:
        data = json.load(f)
    scores = json.loads(data)
    print

    # CREATE => INSERT [POST, GET]
    # insert_query = "INSERT INTO cars VALUES(?, ?, ?, ?, ?, ?, ?);"
    # id = 369
    # brand = 'yugo'
    # model = 'v0.1'
    # year = 1000
    # condition = 'new'
    # price = 1
    # count = 1
    # cursor.execute(insert_query,(id, brand, model, year, condition, price, count))

    # READ => SELECT [GET]
    # print_query = "SELECT * FROM cars"
    # cursor.execute(print_query)
    # for row in cursor.fetchall():
    #     print(row)


    # read_query = "SELECT COUNT(0) FROM cars"
    # cursor.execute(read_query)
    # # count = cursor.fetchone()
    # count = cursor.fetchone()[0]
    # print(count, type(count))



    # UPDATE => UPDATE [PUT]
    # update_query = "UPDATE cars SET id=? WHERE id >= 100;"
    # cursor.execute(update_query, 101)
    # print_query = "SELECT * FROM cars"
    # cursor.execute(print_query)
    # for row in cursor.fetchall():
    #     print(row)


    # DELETE => DELETE [DELETE]
    # 1. Remove the cars which are not on stock.
    # delete_query = "DELETE cars WHERE count=?"
    # cursor.execute(delete_query, 0)
    # print_query = "SELECT * FROM cars"
    # cursor.execute(print_query)
    # for row in cursor.fetchall():
    #     print(row)

    # 1. Remove the cars which are not on stock.
    # delete_query = "DELETE cars WHERE count=?"
    # cursor.execute(delete_query, 0)
    # print_query = "SELECT * FROM cars"
    # cursor.execute(print_query)
    # for row in cursor.fetchall():
    #     print(row)
    # 2. Decrease the price of wrecks by 20%.
    # delete_query = "UPDATE cars SET price=price*0.8 WHERE condition=?"
    # cursor.execute(delete_query, 'wreck')
    # print_query = "SELECT * FROM cars"
    # cursor.execute(print_query)
    # for row in cursor.fetchall():
    #     print(row)
    # 3. Count the average age of the cars on the stock.
    # read_query = "SELECT SUM(2022-year)/COUNT(0) FROM cars"
    # cursor.execute(read_query)
    # average = cursor.fetchone()[0]
    # print(average)


    connection.commit()
    cursor.close()
except pyodbc.OperationalError as e:
    print("SQL Server connection error.")
except pyodbc.ProgrammingError as e:
    print("SQL statement error.")
finally:
    connection.close()
    print('Connection closed')
    # if connection.connected == 1: => this works for pypyodbc
    #     connection.close()
    #     print('Connection closed')

