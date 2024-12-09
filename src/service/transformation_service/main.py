from src.service.controller_service.transformation_controller import TransformController

transformation_controller = TransformController()


# Hàm này dùng để transform data và load data vào warehouse
# Hiện thực code ở thư mục src/service/transform_service
def transforms_data():
    # Lấy cấu từ controller
    transformation_controller.get_config()


if __name__ == '__main__':
    transforms_data()
