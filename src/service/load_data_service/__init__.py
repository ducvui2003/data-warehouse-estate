import os
from fileinput import filename
from pathlib import Path
import mysql.connector
from mysql.connector import Error
from src.service.load_data_service.database_staging import Staging
from src.service.load_data_service.database_staging import Staging
import asyncio


def load_file_to_staging():
    print("Starting execute_sql_with_dynamic_file...")
    try:
        # 1. Kết nối estate controller và gọi procedure get_log_to_loadfile()
        result = Staging().call_controller_procedure('get_log_to_loadfile', ())
        print(result)

        # 2. Kiểm tra File path có tồn tại hay không
        if result:
            file_name = result['data_dir_path'].replace("\\", "/") + '/' + result['file_name']
            print(file_name)

            name_table = None
            if result['resource_id'] == 1:
                name_table = 'estate_daily_temp_batdongsan_com_vn'
            else:
                name_table = 'estate_daily_temp_muaban_net'

            # 2.1 Gọi procedure load_command_file
            try:
                command = Staging().call_staging_procedure('load_command_file', (file_name, name_table,))
                command_sql = command.get('generated_sql', '')
                if not command_sql:
                    raise RuntimeError("Procedure load_command_file did not return valid SQL commands.")
            except Exception as e:
                print(f"Error while calling load_command_file: {e}")
                raise  # Chuyển tiếp lỗi ra ngoài

            # 3. Kết nối DB Staging
            try:
                connector_staging = Staging().get_connection_staging()
                cursor = connector_staging.cursor()

                # 4. Thực thi các câu lệnh trong command_sql
                for statement in command_sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        try:
                            cursor.execute(statement)
                            print(f"Executed: {statement}")
                            while cursor.nextset():
                                pass
                        except Error as sql_error:
                            print(f"SQL execution error: {sql_error}")
                            raise  # Chuyển tiếp lỗi ra ngoài

                connector_staging.commit()
                cursor.close()
            except Error as db_error:
                print(f"Database connection or execution error: {db_error}")
                raise  # Chuyển tiếp lỗi ra ngoài

            # 5. Gọi procedure update_isDelete_loadFile
            try:
                Staging().call_controller_procedure('update_isDelete_loadFile', (result['id'],))
                print("Log status updated successfully.")
            except Exception as update_error:
                print(f"Error while updating log status: {update_error}")
                raise  # Chuyển tiếp lỗi ra ngoài

            print("SQL script with dynamic file path executed successfully.")
        else:
            print('FILE NOT EXIST')
            raise FileNotFoundError("File not found for loading to staging.")
    except Exception as e:
        print(f"Unhandled error: {e}")
        raise  # Chuyển tiếp lỗi ra ngoài



if __name__ == "__main__":
    load_file_to_staging()
