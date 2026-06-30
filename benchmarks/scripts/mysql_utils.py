import mysql.connector
from mysql.connector import Error
import logging

logger = logging.getLogger(__name__)


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        logger.info("Connection to Benchmark Database is successful")
    except Error as e:
        logger.error("The error '%s' occurred", e)

    return connection


def execute_mysql_query(connection, query, cluster_name):
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(query, (cluster_name,))
        result = cursor.fetchall()
        return result
    except Error as e:
        logger.error("The error '%s' occurred", e)
        return []
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Error:
                # Ignore errors on close to avoid masking the original exception
                pass
