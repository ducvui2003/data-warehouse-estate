from src.service.controller_service.transformation_controller import TransformController




# Hàm này dùng để transform data và load data vào warehouse
# Hiện thực code ở thư mục src/service/transform_service


if __name__ == '__main__':
    transformation_controller = TransformController()
    transformation_controller.get_config()
