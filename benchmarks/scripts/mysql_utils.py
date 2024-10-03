import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to Benchmark Database is successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_mysql_query(connection, query, cluster_name):
    cursor = connection.cursor()
    try:
        cursor.execute(query, (cluster_name,))
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
