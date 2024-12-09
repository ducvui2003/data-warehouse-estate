# Hàm này dùng để load data từ staging vào warehouse
# Hiện thực code ở thư mục src/service/load_data_warehourse_service
from src.service.load_data_service import load_file_to_staging


def load_data_from_staging_to_warehouse():
    # Lấy cấu từ controller
    load_file_to_staging()


if __name__ == '__main__':
    load_data_from_staging_to_warehouse()
