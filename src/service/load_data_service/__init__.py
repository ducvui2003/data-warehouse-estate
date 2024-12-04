import os
from pathlib import Path

import mysql.connector
from mysql.connector import Error
from src.service.controller_service.database_controller import Controller
from src.service.load_data_service.database_staging import Staging


class LoadFileIntoStaging:
    def __init__(self):
        self.has_run = False
        self.idConfig = None
    def execute_sql_with_dynamic_file(self, sql_file_path, csv_file_path):

        print("Starting execute_sql_with_dynamic_file...")
        try:
            # 1. connect estate controller (call 1.hàm get_controller_connection)
            __connector_controller = Controller().get_connection_controller()
            print("Connection controller:", __connector_controller)
            __connector_staging = Staging().get_connection_staging()
            file_name = os.path.basename(csv_file_path)
            print("file_name:", file_name)
            status_result = Controller().call_controller_procedure('get_status_by_filename', (file_name,))
            print("Status result:", status_result)
            if status_result and 'status' in status_result:
                status = status_result['status']
                print("Status of file:", status)
            else:
                print(f"No valid status found for file '{file_name}'.")
                status = None
            if status == 'FILE_PENDING':
                # Đọc file SQL và thay thế đường dẫn file động
                with open(sql_file_path, 'r', encoding='utf-8') as file:
                    sql_script = file.read().replace(
                        'C:/Users/ADMIN/Downloads/data_batdongsan_com_vn.csv',
                        csv_file_path
                    )
                    if 'batdongsan_com_vn' not in csv_file_path:
                        sql_script = sql_script.replace(
                            'estate_daily_temp_batdongsan_com_vn',
                            'estate_daily_temp_muaban_net'
                        )

                cursor = __connector_staging.cursor()
                for statement in sql_script.split(';'):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
                        print(f"Executed: {statement}")
                        while cursor.nextset():
                            pass

                __connector_staging.commit()
                print("SQL script with dynamic file path executed successfully.")
                self.update_log_status(__connector_controller, file_name, 'WAREHOUSE_PENDING')
                return  # Dừng lại sau khi đã hoàn tất
            else:
                print("CSV file path not found in the list of pending files.")
                return  # Trả về nếu tệp CSV không có trong danh sách
        except Error as e:
            print(f"Error: {e}")

        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if '__connector_staging' in locals() and __connector_staging.is_connected():
                __connector_staging.close()
        
    def update_log_status(self, connection, csv_file_path, status):
        try:
            cursor = connection.cursor()
            # Gọi thủ tục
            cursor.callproc('update_log_status', (csv_file_path, status))
            # Commit thay đổi
            connection.commit()
            print("Cập nhật thành công.")
        except mysql.connector.Error as err:
            print(f"Lỗi: {err}")
        finally:
            cursor.close()


# Đường dẫn tệp SQL và CSV
sql_file_path = 'C:/Users/ADMIN/Desktop/IT_Data/Data Warehouse/data/load_file_to_staging.sql'
csv_file_path = 'C:/Users/ADMIN/Downloads/data_batdongsan_com_vn_day_2.csv'

# Khởi tạo đối tượng và gọi hàm
loader = LoadFileIntoStaging()
loader.execute_sql_with_dynamic_file(sql_file_path, csv_file_path)
