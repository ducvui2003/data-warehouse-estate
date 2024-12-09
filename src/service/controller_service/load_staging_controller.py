import mysql.connector

from src.config.procedure import get_script_load_file_by_source, insert_log_staging, get_log_staging
from src.exception.AppException import STATUS, AppException
from src.service.controller_service.database_controller import Controller
from src.service.notification_service.email import EmailTemplate, LABEL


class LoadStagingController(Controller):
    def __init__(self):
        super().__init__()

    def get_config(self):
        # 10.2 Gọi hàm call_procedure (4.1) để lấy cấu hình cho load staging
        data = self.call_controller_procedure(get_log_staging, ())
        # 10.3 kiểm tra các thông số cấu hình lấy về data != None
        if data is None:
            # 10.3.1 Không lấy được cấu hình
            return
        # print(data['name'])
        # print(data['file_part'])

        # 10.3.2 Lấy được cấu hình
        # 10.4 Lấy đoạn script load file từ database
        config_id = data['id']
        name = data['name']
        file_path = data['file_path']
        sql_list = self.call_controller_procedure(get_script_load_file_by_source, (name, file_path))
        sql_statement = [obj['sql_statement'] for obj in sql_list]
        connection = self.get_staging_connection()
        cursor: mysql.connector.connection.MySQLCursor = connection.cursor()

        try:
            for sql in sql_statement:
                cursor.execute(sql)
            self.call_controller_procedure(insert_log_staging, (
                config_id, 0, '', "TRANSFORM_PENDING"
            ))
        except AppException as e:
            self.call_controller_procedure(insert_log_staging, (
                config_id, 0, '', "STAGING_ERROR"
            ))
            email_template = EmailTemplate(subject="ERROR",
                                           status=STATUS.STAGING_ERROR.name,
                                           code=STATUS.STAGING_ERROR.value,
                                           message="Error while loading staging",
                                           label=LABEL.ERROR)
            email_template.sent_mail()
