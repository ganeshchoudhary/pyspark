import mysql.connector
from mysql.connector import Error


def mysql_curser():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='kiscore_service',
                                             user='root',
                                             password='root')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            cursor.execute("use kiscore_service;")
            cursor.execute("select * from kiscore_service.score;")
            record = cursor.fetchall()
            print("You're connected to database: ", record)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


if __name__ == '__main__':
    mysql_curser()