import os
from fileinput import filename
from pathlib import Path
import mysql.connector
from mysql.connector import Error

from src.exception.AppException import AppException
# from src.service.load_data_service.database_staging import Staging
from src.service.load_data_service.database_staging import Staging

import asyncio


def load_file_to_staging():
    print("Starting execute_sql_with_dynamic_file...")
    staging = Staging()
    try:
        # **1. Kết nối Staging và gọi procedure get_log_staging()**
        # gọi lai hàm  4.1.hàm call_controller_procedure(DataflowController)

        result = staging.call_controller_procedure('get_log_staging', ())

        # **2. Kiểm tra có kết quả trả về hay không**
        if result:
            config_id = result['config_id']
            file_path = result['file_path']
            error_dir_path = result['error_dir_path']
            file_format = result['file_format']
            prefix = result['prefix']
            resource_id = result['resource_id']
            try:
                print(result)

                # **2.1  Call procedure_staging load_command_file(file_name,table_name) lấy command để thực thi
                # Gọi lại WF 4.2 hàm call_procedure_controller (DataflowController)
                try:
                    command = Staging().call_staging_procedure('load_command_file', (file_path, resource_id,))
                    command_sql = command.get('generated_sql', '')
                    if not command_sql:
                        raise RuntimeError("Procedure load_command_file did not return valid SQL commands.")
                    # **2.2 Raise error nếu không lấy được lệnh SQL**
                except Exception as e:
                    print(f"Error while calling load_command_file: {e}")
                    raise AppException(
                        status="STAGING_ERROR",
                        message=f"Error while calling load_command_file: {e}",
                    )  # Chuyển tiếp lỗi ra ngoài

                # **3. Kết nối DB Staging**
                # 4. Kết nối có thành công hay không ?
                try:
                    connector_staging = Staging().get_connection_staging()
                    cursor = connector_staging.cursor()

                    # **4.1 Thực thi các câu lệnh trong command_sql**
                    for statement in command_sql.split(';'):
                        statement = statement.strip()
                        if statement:
                            try:
                                cursor.execute(statement)
                                print(f"Executed: {statement}")
                                while cursor.nextset():
                                    pass
                            except Error as sql_error:
                                # **4.2 Raise error nếu gặp lỗi khi thực thi SQL**
                                print(f"SQL execution error: {sql_error}")
                                raise AppException(
                                    status="STAGING_ERROR",
                                    message=f"SQL execution error: {sql_error}",
                                )  # Chuyển tiếp lỗi ra ngoài
                    connector_staging.commit()
                    cursor.close()
                except Error as db_error:
                    # **4.2 Raise error nếu kết nối DB thất bại**
                    print(f"Database connection or execution error: {db_error}")
                    raise AppException(
                        status="STAGING_ERROR",
                        message=f"Database connection or execution error: {db_error}",
                    )  # Chuyển tiếp lỗi ra ngoài
                # 5. Kết nối Staging để call_procedure 'update_isDelete_loadFile' để cập nhật isDelete = 1
                # **6. Kiểm tra có update thành công hay không**
                try:
                    # 6.1 Gọi call procedure insert_log_staging('insert_log_staging', (config_id, 0, '', 'TRANSFORM_PENDING') để insert log
                    # gọi lai hàm  4.1.hàm call_controller_procedure(DataflowController)
                    Staging().call_controller_procedure('insert_log_staging', (config_id, 0, '', 'TRANSFORM_PENDING'))
                    print("Log status updated successfully.")
                except AppException as update_error:
                    # **6.2 Raise error nếu cập nhật thất bại**
                    print(f"Error while updating log status: {update_error}")
                    raise AppException(
                        status="STAGING_ERROR",
                        message=f"Error while updating log status: {update_error}",
                    )  # Chuyển tiếp lỗi ra ngoài

                print("SQL script with dynamic file path executed successfully.")
            except AppException as e:
                # **2.2 Raise error nếu không có file để load**
                print(f"Unhandled error: {e}")
                file_name = f"{prefix}{file_format}.log"
                file_path = os.path.join(error_dir_path, file_name)
                handle_error(e, staging, config_id, file_path, 'STAGING_ERROR')
        else:
            # **2.2 Raise error nếu không có file để load**
            print('FILE NOT EXIST')
            raise FileNotFoundError("File not found for loading to staging.")
    except Exception as e:
        # **Catch lỗi tổng quát từ bất kỳ bước nào**
        print(f"Unhandled error: {e}")


def handle_error(exception: AppException,
                 staging,
                 config_id,
                 error_file_name,
                 status):
    exception.file_error = error_file_name
    exception.handle_exception()
    staging.call_controller_procedure("insert_log_staging", (config_id, 0, error_file_name, status))


if __name__ == "__main__":
    load_file_to_staging()
