from src.service.load_data_service.database_staging import Staging
from database_staging import Staging

class EstateDataLoader:
    def __init__(self):
        self.staging = Staging()

    def load_data(self):
        try:
            # ** gọi procedure để lấy file cần load**
            result = self.staging.call_controller_procedure('get_log_to_loadfile', ())

            # Kiểm tra kết quả trả về từ stored procedure
            if not result:
                print("Không có file nào cần load.")
                return

            # Kiểm tra và lấy thông tin file
            data_dir_path = result.get('data_dir_path', None)
            file_name = result.get('file_name', None)
            if not data_dir_path or not file_name:
                print(f"Lỗi: Không tìm thấy thông tin đường dẫn file hoặc tên file. Kết quả trả về: {result}")
                return

            # Tạo đường dẫn file
            file_path = data_dir_path.replace("\\", "/") + '/' + file_name
            print(f"Đang xử lý file: {file_path}")

            # ** gọi procedure để lấy câu lệnh SQL cần thực thi**
            command = self.staging.call_staging_procedure('load_command_file', (file_path, result['resource_id'],))
            command_sql = command.get('generated_sql', '')

            # Kiểm tra câu lệnh SQL
            if not command_sql:
                raise RuntimeError("Procedure load_command_file không trả về câu lệnh SQL hợp lệ.")

            # ** thực thi các câu lệnh SQL trên database staging**
            connector_staging = self.staging.get_connection_staging()
            cursor = connector_staging.cursor()

            # Thực thi từng câu lệnh SQL
            for statement in command_sql.split(';'):
                statement = statement.strip()
                if statement:
                    try:
                        cursor.execute(statement)
                        print(f"Đã thực thi: {statement}")
                        while cursor.nextset():  # Xử lý kết quả tiếp theo (nếu có)
                            pass
                    except Exception as sql_error:
                        print(f"Lỗi khi thực thi SQL: {sql_error}")
                        raise

            # Commit thay đổi sau khi thực thi
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
            # ** Lấy dữ liệu từ bảng estate_daily trong staging**
            staging_query = """
            SELECT estate_name, location, price, area
            FROM estate_daily
            WHERE price > 0; -- Điều kiện lọc
            """
            data = self.staging.call_staging_query(staging_query)

            # Kiểm tra kết quả trả về
            if not data:
                print("Không có dữ liệu cần tải sang Warehouse.")
                return

            # ** Chuẩn bị dữ liệu để chèn vào warehouse**
            warehouse_insert_query = """
            INSERT INTO estate (estate_name, location, price, area, load_timestamp)
            VALUES (%s, %s, %s, %s, NOW());
            """
            insert_data = [
                (row['estate_name'], row['location'], row['price'], row['area'])
                for row in data
            ]

            # Kiểm tra dữ liệu trước khi in log
            if insert_data:
                print(f"Đang tải {len(insert_data)} dòng dữ liệu vào warehouse...")

            # Thực thi câu lệnh chèn dữ liệu
            self.staging.call_warehouse_query(warehouse_insert_query, insert_data)

            print(f"Đã tải {len(data)} dòng dữ liệu từ estate_staging sang estate_warehouse thành công.")
        except Exception as e:
            print(f"Lỗi khi tải dữ liệu sang Warehouse: {e}")
            raise


if __name__ == "__main__":
    loader = EstateDataLoader()

    try:
        print("Bắt đầu tải dữ liệu từ file staging...")
        loader.load_data()

        print("Chuyển đổi và tải dữ liệu sang warehouse...")
        loader.transform_and_load_to_warehouse()

    except Exception as e:
        print(f"Quá trình tải dữ liệu gặp lỗi: {e}")
