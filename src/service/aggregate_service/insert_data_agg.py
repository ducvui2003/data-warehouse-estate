from src.service.aggregate_service.database_warehouse import Warehouse  # Import class Warehouse

def insert_data_aggregate():
    try:
        # Tạo một instance của lớp Warehouse để kết nối đến cơ sở dữ liệu
        warehouse = Warehouse()

        # Gọi thủ tục insert_into_aggregate_by_district_month
        print("Gọi thủ tục: insert_into_aggregate_by_district_month")
        warehouse.call_warehouse_procedure('insert_into_aggregate_by_district_month', (), None)
        print("Thủ tục insert_into_aggregate_by_district_month đã thực thi thành công!")

        # Gọi thủ tục insert_into_aggregate_by_province_month
        print("Gọi thủ tục: insert_into_aggregate_by_province_month")
        warehouse.call_warehouse_procedure('insert_into_aggregate_by_province_month', (), None)
        print("Thủ tục insert_into_aggregate_by_province_month đã thực thi thành công!")

    except Exception as e:
        print(f"Lỗi trong quá trình thực thi thủ tục: {e}")
    finally:
        print("Quá trình thực thi kết thúc.")

if __name__ == "__main__":
    insert_data_aggregate()
