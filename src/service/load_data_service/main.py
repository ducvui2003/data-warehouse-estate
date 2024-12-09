# Hàm này dùng để load data từ staging vào warehouse
# Hiện thực code ở thư mục src/service/load_data_warehourse_service
from src.service.controller_service.load_staging_controller import LoadStagingController

if __name__ == '__main__':
    c = LoadStagingController()
    c.get_config()
