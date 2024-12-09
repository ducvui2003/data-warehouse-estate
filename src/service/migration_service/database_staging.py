from src.config.database import MySQLCRUD
from src.config.setting import CONTROLLER_DB_PORT, CONTROLLER_DB_HOST, CONTROLLER_DB_NAME, CONTROLLER_DB_USER, \
    CONTROLLER_DB_PASS, CONTROLLER_DB_POOL_NAME, CONTROLLER_DB_POOL_SIZE


class Staging:
    __connector: MySQLCRUD = None

    def __init__(self):
        self.__connector = MySQLCRUD(
            host=CONTROLLER_DB_HOST,
            port=CONTROLLER_DB_PORT,
            database=CONTROLLER_DB_NAME,
            user=CONTROLLER_DB_USER,
            password=CONTROLLER_DB_PASS,
            pool_name=CONTROLLER_DB_POOL_NAME,
            pool_size=CONTROLLER_DB_POOL_SIZE
        )

        print(f"Connection pool created with pool size: {CONTROLLER_DB_POOL_SIZE}")


    def call_staging_query(self, query):
        connection = self.__connector.get_staging_connection()
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            connection.close()

    def call_warehouse_query(self, query, data=None):
        connection = self.__connector.get_warehouse_connection()
        try:
            cursor = connection.cursor()
            if data:
                cursor.executemany(query, data)
            else:
                cursor.execute(query)
            connection.commit()
        finally:
            cursor.close()
            connection.close()

    def call_controller_procedure(self, procedure_name, args):
        connection = self.__connector.get_controller_connection()
        result = self.__connector.call_procedure(procedure_name, connection, args)
        # connection.close()
        return result

    def call_staging_procedure(self, procedure_name, args):
        connection = self.__connector.get_staging_connection()
        result = self.__connector.call_procedure(procedure_name, connection, args)
        # connection.close()
        return result

    def get_connection_staging(self):
        connection = self.__connector.get_warehouse_connection()
        return connection
