import os
from fileinput import filename
from pathlib import Path
import mysql.connector
from mysql.connector import Error
from src.service.controller_service.database_controller import Controller
from src.service.load_data_service.database_staging import Staging


def load_file_to_staging():
    print("Starting execute_sql_with_dynamic_file...")
    try:
        # 1. kết nối estate controller and call procedure get_log_to_loadfile()  to get filepath
        # *Dùng 4.1 hàm call_controller_procedure của Dataflow(Controller)*
        result = Controller().call_controller_procedure('get_log_to_loadfile', ())
        print(result)
        # 2. Kiểm tra File path có tồn tại hay không
        if result:
            # file_name = (result['data_dir_path'] + '\\'+ result['file_name'])
            file_name = result['data_dir_path'].replace("\\", "/") + '/' + result['file_name']
            print(file_name)
            name_table = None
            if(result['resource_id'] == 1):
                name_table = 'estate_daily_temp_batdongsan_com_vn'
            else:
                name_table = 'estate_daily_temp_muaban_net'
            # 2.1 Yes: Call procedure load_command_file(file_name,table_name) lấy command để thực thi
            command = Controller().call_staging_procedure('load_command_file', (file_name,name_table,))
            command_sql = command.get('generated_sql', '')
            if not command_sql:
                print("No SQL command generated!")
                return
            # 3. Kết nối Db Staging
            try:
                connector_staging = Staging().get_connection_staging()
                cursor = connector_staging.cursor()
                # 4. thực thi các câu lệnh trong command
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
                            continue  # Tiếp tục với câu lệnh SQL khác
                connector_staging.commit()
                cursor.close()
            except Error as e:
                print(f"Database connection or execution error: {e}")

            id = result['id']
            # 5. Update logs.status = 'STAGING_PENDING'
            try:
                Controller().call_controller_procedure('update_log_loadFile', (id, 'TRANSFORM_PENDING'))
                print("Log status updated successfully.")
            except Error as e:
                print(f"Error while updating log: {e}")

            print("SQL script with dynamic file path executed successfully.")
            return
            # 2.2 No
        else:
            print("SEND MAIL ERROR.")
            return

    except Error as e:
        print(f"Error: {e} ")




load_file_to_staging()
