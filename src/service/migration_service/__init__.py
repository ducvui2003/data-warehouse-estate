
from src.service.load_data_service.database_staging import Staging
from database_staging import Staging


class EstateDataLoader:
    def __init__(self):
        self.staging = Staging()

    def load_data(self):
        try:
            # ** gọi procedure để lấy file cần load**
            result = self.staging.call_controller_procedure('get_log_to_loadfile', ())
            if not result:
                print("Không có file nào cần load.")
                return

            file_name = result['data_dir_path'].replace("\\", "/") + '/' + result['file_name']
            print(f"Đang xử lý file: {file_name}")

            # ** gọi procedure để lấy câu lệnh sql cần thực thi**
            command = self.staging.call_staging_procedure('load_command_file', (file_name, result['resource_id'],))
            command_sql = command.get('generated_sql', '')

            if not command_sql:
                raise RuntimeError("Procedure load_command_file không trả về câu lệnh SQL hợp lệ.")

            # ** thực thi các câu lệnh sql trên database staging**
            connector_staging = self.staging.get_connection_staging()
            cursor = connector_staging.cursor()
            for statement in command_sql.split(';'):
                statement = statement.strip()
                if statement:
                    try:
                        cursor.execute(statement)
                        print(f"Đã thực thi: {statement}")
                        while cursor.nextset():
                            pass
                    except Exception as sql_error:
                        print(f"Lỗi khi thực thi SQL: {sql_error}")
                        raise

            connector_staging.commit()
            cursor.close()

            # ** gọi procedure để cập nhật trạng thái file**
            self.staging.call_controller_procedure('update_isDelete_loadFile', (result['id'],))
            print("Cập nhật trạng thái log thành công.")

        except Exception as e:
            print(f"Lỗi khi load dữ liệu: {e}")
            raise

    def transform_and_load_to_warehouse(self):
        try:
            # ** lấy dữ liệu từ bảng estate_daily trong staging**
            staging_query = """
            SELECT estate_name, location, price, area
            FROM estate_daily
            WHERE price > 0; -- Điều kiện lọc
            """
            data = self.staging.call_staging_query(staging_query)

            if not data:
                print("Không có dữ liệu cần tải sang Warehouse.")
                return

            # ** chèn dữ liệu vào bảng estate trong warehouse**
            warehouse_insert_query = """
            INSERT INTO estate (estate_name, location, price, area, load_timestamp)
            VALUES (%s, %s, %s, %s, NOW());
            """
            insert_data = [(row['estate_name'], row['location'], row['price'], row['area']) for row in data]
            self.staging.call_warehouse_query(warehouse_insert_query, insert_data)

            print(f"Đã tải {len(data)} dòng dữ liệu từ estate_staging sang estate_warehouse thành công.")
        except Exception as e:
            print(f"Lỗi khi tải dữ liệu sang Warehouse: {e}")
            raise


if __name__ == "__main__":
    loader = EstateDataLoader()

    # ** thực thi tải dữ liệu từ file và chuyển đổi**
    try:
        print("Bắt đầu tải dữ liệu từ file staging...")
        loader.load_data()

        print("Chuyển đổi và tải dữ liệu sang warehouse...")
        loader.transform_and_load_to_warehouse()
    except Exception as e:
        print(f"Quá trình tải dữ liệu gặp lỗi: {e}")

