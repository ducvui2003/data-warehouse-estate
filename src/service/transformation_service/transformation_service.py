import os

from src.config.procedure import transform_batdongsan_com_vn, transform_muaban_net, \
    load_staging_warehouse_batdongsan_com_vn, load_staging_warehouse_muaban_net
from src.exception.AppException import AppException, STATUS
from src.service.controller_service.database_controller import Controller
from src.service.notification_service.email import EmailTemplate, LABEL


class Transformation:
    def __init__(self,
                 source_name,
                 controller: Controller,
                 prefix,
                 file_format,
                 error_dir_path):
        self._source = source_name
        self._controller = controller
        self._prefix = prefix
        self._file_format = file_format
        self._error_dir_path = error_dir_path

    def handle(self):
        count_row = 0
        try:
            if self._source == 'batdongsan.com.vn':
                self._controller.call_staging_procedure(transform_batdongsan_com_vn, ())
                count_row = self._controller.call_staging_procedure(load_staging_warehouse_batdongsan_com_vn, ())
            elif self._source == 'muaban.net/bat-dong-san':
                self._controller.call_staging_procedure(transform_muaban_net, ())
                count_row = self._controller.call_staging_procedure(load_staging_warehouse_muaban_net, ())
            else:
                raise AppException(message='Source not found')
            return self.handle_success(count_row)
        except AppException  as e:
            return self.handle_exception(e)

    def handle_success(self, count_row):
        email_template = EmailTemplate(subject="Sent data to warehouse",
                                       status=STATUS.STAGING_PENDING.name,
                                       code=STATUS.STAGING_PENDING.value,
                                       message="Success",
                                       file_log=None,
                                       label=LABEL.INFO)
        email_template.sent_mail()
        return {
            'file': None,
            'error_file_name': None,
            'count_row': count_row,
            'status': 'STAGING_SUCCESS'
        }

    def handle_exception(self, exception: AppException):
        # 11.1 Tạo file name error
        filename = f"{self._prefix}{self._file_format}.log"
        path = os.path.join(self._error_dir_path, filename)
        # 11.2 cài đặt file name error vào exception
        exception.file_error = filename
        exception._status = STATUS.STAGING_ERROR
        # 11.3 gọi hàm handler_exception trong exception (15)
        exception.handle_exception()
        # 11.4 Trả về giá trị gồm file, error file name, count row, status
        return {
            'file': None,
            'error_file_name': path,
            'count_row': 0,
            'status': 'STAGING_ERROR'
        }
