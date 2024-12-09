from src.config.procedure import insert_new_log_crawler
from src.service.controller_service.database_controller import Controller


class InsertLogCrawler(Controller):
    def __init__(self):
        super().__init__()

    def insert_log_crawler(self):
        return self.call_controller_procedure(insert_new_log_crawler,())