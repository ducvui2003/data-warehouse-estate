from src.service.aggregate_service.database_warehouse import Warehouse  # Import class Warehouse

def insert_data_aggregate():
    try:
        # 1. Tạo đối tượng Warehouse để kết nối cơ sở dữ liệu
        warehouse = Warehouse()

        procedure = "insert_into_aggregate_by_district_month"  # Thủ tục tổng hợp theo quận và tháng
        try:
            # 3. Gọi thủ tục cần thực thi bằng hàm call_warehouse_procedure
            print(f"Gọi thủ tục: {procedure}")
            warehouse.call_warehouse_procedure(procedure, (), None)
            # 4.1 Thông báo gọi thủ tục thành công
            print(f"Thủ tục '{procedure}' đã thực thi thành công!")
        except Exception as e:
            # 4.2.Thông báo lỗi khi gọi thủ tục không thành công
            print(f"Lỗi khi gọi thủ tục '{procedure}': {e}")

    except Exception as e:
        print(f"Error during the execution of procedure: {e}")
    finally:
        # 5. Kết thúc quá trình tổng hợp
        print("Data aggregation process has completed.")

if __name__ == "__main__":
    # Gọi hàm chính để bắt đầu quá trình tổng hợp dữ liệu
    insert_data_aggregate()
