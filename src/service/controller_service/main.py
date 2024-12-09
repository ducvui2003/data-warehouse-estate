from src.service.controller_service.crawl_controller import CrawlController
from src.config.procedure import insert_new_log_crawler_daily as insert_new_log_crawler_daily_procedure

crawl_controller = CrawlController()


def insert_new_log_crawler_daily():
    crawl_controller.call_controller_procedure(insert_new_log_crawler_daily_procedure, ())


if __name__ == '__main__':
    insert_new_log_crawler_daily()
