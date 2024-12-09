from src.service.controller_service.crawl_controller import CrawlController

crawl_controller = CrawlController()


def crawl_data():
    # 6. Crawl_Data:
    # 6.1. sử hàm getConfig của crawlController đã tạo trước đó
    crawl_controller.get_config()


# > python -m src.service.extract_service.main
if __name__ == '__main__':
    crawl_data()
