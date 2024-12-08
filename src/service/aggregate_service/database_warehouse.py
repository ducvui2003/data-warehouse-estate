from src.config.database import MySQLCRUD
from src.config.setting import CONTROLLER_DB_PORT, CONTROLLER_DB_HOST, CONTROLLER_DB_NAME, CONTROLLER_DB_USER, \
    CONTROLLER_DB_PASS, CONTROLLER_DB_POOL_NAME, CONTROLLER_DB_POOL_SIZE


class Warehouse:
    __connector: MySQLCRUD = None

    def __init__(self):
        try:
            # Khởi tạo kết nối với cơ sở dữ liệu qua MySQLCRUD
            self.__connector = MySQLCRUD(
                host=CONTROLLER_DB_HOST,
                port=CONTROLLER_DB_PORT,
                database=CONTROLLER_DB_NAME,
                user=CONTROLLER_DB_USER,
                password=CONTROLLER_DB_PASS,
                pool_name=CONTROLLER_DB_POOL_NAME,
                pool_size=CONTROLLER_DB_POOL_SIZE
            )
            print(f"Pool kết nối đã được tạo với kích thước pool: {CONTROLLER_DB_POOL_SIZE}")
        except Exception as e:
            print(f"Đã xảy ra lỗi khi tạo pool kết nối: {e}")
            self.__connector = None  # Đảm bảo __connector được gán None nếu có lỗi

    def call_warehouse_procedure(self, procedure_name, args, header):
        # Kiểm tra nếu __connector không được khởi tạo thì bỏ qua việc gọi thủ tục
        if not self.__connector:
            print("Connector chưa được khởi tạo. Bỏ qua việc gọi thủ tục.")
            return

        connection = None
        try:
            # Lấy kết nối từ pool
            connection = self.__connector.get_warehouse_connection()
            if connection:
                print(f"Kết nối đến cơ sở dữ liệu {CONTROLLER_DB_NAME} đã được thiết lập.")
                # Gọi thủ tục và nhận kết quả
                result = self.__connector.call_procedure(procedure_name, connection, args)
                return result
            else:
                raise Exception("Không thể thiết lập kết nối đến cơ sở dữ liệu.")
        except Exception as e:
            print(f"Đã xảy ra lỗi khi gọi thủ tục {procedure_name}: {e}")
        finally:
            if connection:
                # Kết nối sẽ được trả lại pool tự động, không cần gọi connection.close()
                pass
