from src.service.aggregate_service.database_warehouse import (Warehouse)

def insert_data_aggregate():
    try:
        # Tạo đối tượng Warehouse để kết nối cơ sở dữ liệu
        warehouse = Warehouse()

        # Danh sách các thủ tục cần thực thi
        procedures = [
            "insert_into_aggregate_by_district_month",  # Thủ tục tổng hợp theo quận và tháng
            "insert_into_aggregate_by_province_month",  # Thủ tục tổng hợp theo tỉnh và tháng
        ]

        # Thực thi các thủ tục trong danh sách
        for procedure in procedures:
            try:
                print(f"Gọi thủ tục: {procedure}")
                warehouse.call_warehouse_procedure(procedure, (), None)
                print(f"Thủ tục '{procedure}' đã thực thi thành công!")
            except Exception as e:
                print(f"Lỗi khi gọi thủ tục '{procedure}': {e}")

    except Exception as e:
        # Báo lỗi nếu có vấn đề trong quá trình tổng hợp dữ liệu
        print(f"Error during the execution of procedures: {e}")
    finally:
        # Thông báo khi kết thúc quá trình tổng hợp
        print("Data aggregation process has completed.")

if __name__ == "__main__":
    # Gọi hàm chính để bắt đầu quá trình tổng hợp dữ liệu
    insert_data_aggregate()
